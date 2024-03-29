require 'rubygems'
require 'bud'
require 'membership/membership'
require 'causality/version_vector'
require 'vote/voting'
require 'alarm/alarm'
require 'read_repair/read_repair'
require 'session_guarantees/session_guarantee_voting'
require 'session_guarantees/session_guarantee_client'
require 'gossip/gossip'
require 'counter/sequences'

# Serializes several (vector, value) pairs into one field
module VectorValueMatrixSerializerProtocol
  state do
    # Serialize
    interface input, :serialize, [:request, :v_vector] => [:value]
    interface output, :serialize_ack, [:request] => [:matrix]
    # Deserialize
    interface input, :deserialize, [:request] => [:matrix]
    interface output, :deserialize_ack, [:request, :v_vector] => [:value]
  end
end

module VectorValueMatrixSerializer
  include VectorValueMatrixSerializerProtocol

  bloom do
    serialize_ack <= serialize.reduce({}) do |meta, x|
      meta[x.request] ||= []
      meta[x.request] << [x.v_vector, x.value]
      meta[x.request].sort
      meta
    end
    deserialize_ack <= deserialize.flat_map do |x|
      x.matrix.map do |y|
        [x.request, y[0], y[1]]
      end
    end
  end

end

# Performs read/version/write operations on a specified agent.
#
# The results of a request on a specific agent will be wholly
# contained (meaning not split up across timesteps) at some
# timestep in the future.
#
# Basically, this module takes care of running an operation
# on a remote agent and ensuring that the output of that operation
# is rendered to this ack interface in one piece, and rendered only
# once on that ack interface.
module QuorumRemoteProcedureProtocol

  # Request MUST be unique across all read/version/write operations
  # Agent is a network identifier.

  state do
    # Remember key constraint on request
    # interface input, :make_request, [:request] => []

    # Read Operation
    interface input, :read, [:request, :agent] => [:key]
    interface output, :read_ack,  [:request, :agent, :v_vector] => [:value]

    # Version Query
    interface input, :version_query, [:request, :agent] => [:key]
    interface output, :version_ack, [:request, :agent, :v_vector] => []

    # Write Operation
    interface input, :write, [:request, :agent] => [:key, :v_vector, :value]
    interface output, :write_ack, [:request, :agent] => []
  end

end

module QuorumRemoteProcedure
  include QuorumRemoteProcedureProtocol
  import VersionVectorKVS => :vvkvs
  import VersionMatrixSerializer => :vms
  import VectorValueMatrixSerializer => :vvms
  import GossipProtocol => :gp
  import StaticMembership => :sm
  import Counter => :c

  state do
    table :pending_request, [:request] => []
    table :pending_response, [:request] => []
    table :member, [:host] => [:ident]

    channel :read_request, [:@dst, :src, :request] => [:key]
    channel :read_response, [:dst, :@src, :request] => [:matrix]
    channel :version_request, [:@dst, :src, :request] => [:key]
    channel :version_response, [:dst, :@src, :request] => [:v_matrix]
    channel :write_request, \
      [:@dst, :src, :request] => [:key, :v_vector, :value]
    channel :write_response, [:dst, :@src, :request] => []

    # Periodic interval for gossip protocol
    periodic :gossip_timer, 1
    scratch :gossip_kvvalue, vvkvs.kv_store.schema
  end

  # pass of member too the vvkvs
  bloom do
    vvkvs.member <= member
  end

  # Logic to send key, value, vector pairs into gossip protocol
  bloom do
    gossip_kvvalue <= vvkvs.kv_store.group([], choose_rand)
    c.increment_count <= [["gossip"]] if gossip_timer.exists?
    c.get_count <= [["gossip"]]
    gp.send_message <= (gossip_kvvalue * c.return_count).pairs { |l, r|
      if gossip_timer.exists?
        [[l.key, l.v_vector, l.value], r.tally]
      end
    }
    vvkvs.write <= (gp.recv_message * c.return_count).pairs { |l, r|
        [ip_port + r.tally, l.message[0], l.message[1], l.message[2]]
    }
  end

  # Logic to prevent duplicate delivery of acks!
  # It is not well known that channels will sometimes deliver messages twice
  bloom do
    pending_response <= read { |r| [r.request] }
    pending_response <= write { |w| [w.request] }
    pending_response <= version_query { |v| [v.request] }

    pending_response <- (pending_response * read_response)\
      .pairs(:request => :request)
    pending_response <- (pending_response * version_response)\
      .pairs(:request => :request)
    pending_response <- (pending_response * write_response)\
      .pairs(:request => :request)
  end

  # Logic to play nice with other users of the vvkvs!
  bloom do
    # Keep track of requests we'll make on the vvkvs
    pending_request <= read_request { |r| [[r.src, r.request]] }
    pending_request <= version_request { |v| [[v.src, v.request]] }
    pending_request <= write_request { |w| [[w.src, w.request]] }

    # Forget about pending requests that have acked!
    pending_request <- (pending_request * vvkvs.read_ack)\
      .lefts(:request => :request)
    pending_request <- (pending_request * vvkvs.write_ack)\
      .lefts(:request => :request)
    pending_request <- (pending_request * vvkvs.version_ack)\
      .lefts(:request => :request)
  end

  # Send out read operation request and ack
  bloom do
    read_request <~ read do |r|
      [r.agent, ip_port, r.request, r.key]
    end
    vvms.deserialize <= (read_response * pending_response)\
      .lefts(:request => :request) do |r|
      [[r.request, r.dst], r.matrix]
    end
    read_ack <= vvms.deserialize_ack do |a|
      [a.request[0], a.request[1], a.v_vector, a.value]
    end
  end

  # Reply to read operation requests to members
  bloom do
    vvkvs.read <= read_request do |r|
      [[r.src, r.request], r.key]
    end
    vvms.serialize <= (vvkvs.read_ack * pending_request)\
      .lefts(:request => :request)
    read_response <~ vvms.serialize_ack do |a|
      [ip_port, a.request[0], a.request[1], a.matrix]
    end
  end

  # Send out version operation request and ack
  bloom do
    version_request <~ version_query do |x|
      [x.agent, ip_port, x.request, x.key]
    end
    vms.deserialize <= (version_response * pending_response)\
      .lefts(:request => :request) do |x|
      [[x.request, x.dst], x.v_matrix]
    end
    version_ack <= vms.deserialize_ack do |a|
      [a.request[0], a.request[1], a.v_vector]
    end
  end

  # Reply to version operation request
  bloom do
    vvkvs.version_query <= version_request do |v|
      [[v.src, v.request], v.key]
    end
    vms.serialize <= (vvkvs.version_ack * pending_request)\
      .lefts(:request => :request)
    version_response <~ vms.serialize_ack do |a|
      [ip_port, a.request[0], a.request[1], a.v_matrix]
    end
  end

  # Send out write operation requests from members and ack
  bloom do
    write_request <~ write do |w|
      [w.agent, ip_port, w.request, w.key, w.v_vector, w.value]
    end
    write_ack <= (write_response * pending_response)\
      .lefts(:request => :request) do |w|
      [w.request, w.dst]
    end
  end

  # Reply to write operation requests from members
  bloom do
    vvkvs.write <= write_request do |w|
      [[w.src, w.request], w.key, w.v_vector, w.value]
    end
    write_response <~ (vvkvs.write_ack * pending_request)\
      .lefts(:request => :request) do |a|
      [ip_port, a.request[0], a.request[1]]
    end
  end

end


module RWTimeoutQuorumAgentProtocol
  state do
    # requests must be unique among all operations
    # each request must also specify parameters
    # a put request will have the same timeout duration for its internal get version operation and its subsequent write operation
    interface input, :get, [:request] => [:key]
    interface input, :put, [:request] => [:key, :value]
    interface input, :get_version, [:request] => [:key]

    # ack_num is number of acks to wait for to declare victory
    # duration is the time in units of 0.1s to wait until failure
    interface input, :parameters, [:request] => [:ack_num, :duration]

    # states are - :success, :fail, :in_progress
    # NOTE: output responses do not persist after fail or success is reached
    interface output, :status, [:request] => [:state]
    interface output, :get_responses, [:request, :agent, :v_vector] => [:value]
    interface output, :put_responses, [:request, :agent] => []
    interface output, :version_responses, [:request, :agent, :v_vector] => []
  end
end

# VERY NOTEWORTHY THINGS
# 1) We assume that values are always arrays. Even if you are writing a single string "val" into "key, the put request looks like: put into key "key", the value ["val"]
# 2) Users of the RWTimeoutQuorumAgent (namely clients) can expect to use it in the following way:

# 1) perform a request: get, put, get_version
# 2) the partial responses will be poplated in the get, put, and version output interfaces
# 3) when status outputs success, there will be at least as many responses as there are ack nums as specified in parameters
# 3b) when status outputs fail, the output interfaces will still contain partial responses, but they will no longer be output in subsequent timesteps

# Output interfaces do not persist after success or fail, but they will continuously stream a partial set of acks while an operation is in progress
module RWTimeoutQuorumAgent
  #include Bud
  include RWTimeoutQuorumAgentProtocol
  include StaticMembership
  include CountVoteCounter
  import Alarm => :alarm
  import QuorumRemoteProcedure => :rp
  import ReadRepair => :rr
  import VersionVectorMerge => :vvm
  import VersionVectorSerializer => :vvs

  state do
    table :read_acks, [:request, :agent, :v_vector] => [:value]
    table :version_acks, [:request, :agent, :v_vector] => []
    table :write_acks, [:request, :agent]

    # cached puts that are still waiting for version queries
    table :pending_puts, [:request] => [:key, :value]
    # puts that have an updated version vector, and synchronized values
    table :ready_puts, [:request] => [:key, :v_vector, :value]
    # remember the key in a get request since read acks don't have that info
    table :get_cache, [:request] => [:key]
    # remember own address since local id in membership protocol is an id instead of a host
    table :my_addr, []=>[:host]

    # for incrementing coordinator node in a write
    scratch :incremented_coordinators, [:request, :server] => [:version]
  end

  # MISC Logic block
  bloom do
    # cache puts if we are waiting for versions to be read
    pending_puts <= put
    # remember local address
    my_addr <= (local_id * member).matches {|l,r| [r.host]}
    # cache gets for read repair
    get_cache <= get
    # pass off member to rp module
    rp.member <= member
  end

  # setup num candidates, set voting paramters, begin vote and set alarm
  bloom do
    # setup num expected voters
    begin_vote <= get {|g| [g.request, 0xffffffff]}
    begin_vote <= get_version {|g| [g.request, 0xffffffff]}
    begin_vote <= ready_puts {|p| [p.request, 0xffffffff]}
    num_required <= (get * parameters).pairs(:request => :request){|x,p|[x.request, p.ack_num]}
    num_required <= (get_version * parameters).pairs(:request => :request) {|x,p|[x.request, p.ack_num]}
    num_required <= (ready_puts * parameters).pairs(:request => :request){|x,p| [x.request, p.ack_num]}
    # start new vote for write if get_version before write succeeded on this timestep
    num_required <+ (ongoing_ballots * result).pairs(:ballot_id=>:ballot_id) do |l,r|
      [l.ballot_id, l.num_required] if pending_puts.exists? {|pp| pp.request == l.ballot_id} and r.status == :success
    end
    alarm.set_alarm <= (get * parameters).pairs(:request => :request) {|x,p|[x.request, p.duration]}
    alarm.set_alarm <= (get_version * parameters).pairs(:request => :request) {|x,p| [x.request, p.duration]}
    alarm.set_alarm <= (ready_puts * parameters).pairs(:request => :request) {|x,p|[x.request, p.duration]}
  end

  # invoke remote procedures
  bloom do
    rp.read <= (get * member).pairs {|g,m| [g.request, m.host, g.key]}
    rp.version_query <= (get_version * member).pairs {|g,m| [g.request, m.host, g.key]}
    rp.write <= (ready_puts * member).pairs {|p,m| [p.request, m.host, p.key, p.v_vector, p.value]}
  end

  # collect remote procedure return values, record votes!
  bloom do
    # put acks in cast_vote
    # TODO: Make sure timed out acks can't vote or get cached
    cast_vote <= rp.read_ack {|rr| [rr.request, rr.agent, "ack", "read ack"]}
    cast_vote <= rp.version_ack {|vr| [vr.request, vr.agent, "ack", "version ack"]}
    cast_vote <= rp.write_ack {|wr| [wr.request, wr.agent, "ack", "write ack"]}
    # cache remote procedure return values
    read_acks <= rp.read_ack
    version_acks <= rp.version_ack
    write_acks <= rp.write_ack
  end

  # handling results and timeouts
  bloom do
     # timer runs out, vote not done output error
    status <= alarm.alarm do |a|
      [a.ident, :fail] if not result.exists?{|r| r.ballot_id == a.ident}
    end

    # timer runs out, vote finishes simultaneously output success
    status <= alarm.alarm do |a|
      [a.ident, :success] if result.exists? {|r| r.ballot_id == a.ident}
    end

    # vote successful, timer still going
    status <= (result * alarm.countdowns).pairs(:ballot_id=>:ident) do |l,r|
      [l.ballot_id, :success] if l.status == :success and not pending_puts.exists? do |pp|
        pp.request == l.ballot_id
      end
    end

    # clear timer if vote was successful, but not if the vote was for the get version phase of a write
    alarm.stop_alarm <= (result * alarm.countdowns).pairs(:ballot_id=>:ident) do |l,r|
      [l.ballot_id] if l.status == :success and not pending_puts.exists? do |pp|
        pp.request == l.ballot_id
      end
    end

    # clear cached data
    pending_puts <- (pending_puts * result).pairs(:request=>:ballot_id) do |l,r|
      l if r.status == :success or r.status == :fail
    end
    ready_puts <- (ready_puts * status).pairs(:request=>:request) do |l,r|
      l if r.state == :success or r.state == :fail
    end
    read_acks <- (read_acks * status).pairs(:request=>:request) do |l,r|
      l if r.state == :success or r.state == :fail
    end
    version_acks <- (version_acks * result).pairs(:request=>:ballot_id) do |l,r|
      l if r.status == :success or r.status == :fail
    end
    write_acks <- (write_acks * result).pairs(:request=>:ballot_id) do |l,r|
      l if r.status == :success or r.status == :fail
    end
    get_cache <- (get_cache * status).pairs(:request=>:request) do |l,r|
      l if r.state == :success or r.state == :fail
    end
  end

  # handle output
  bloom do
    get_responses <= read_acks
    put_responses <= write_acks
    version_responses <= version_acks

    # do read_repair silently
    rr.read_acks <= (status * read_acks).pairs(:request=>:request) {|l,r| [r.request, r.v_vector, r.value] if l.state == :success}

    rr.read_requests <= (read_acks * get_cache * my_addr * status).combos(read_acks.request=>get_cache.request, get_cache.request=>status.request) do |r,g,a,s| 
      [r.request, g.key, a.host] if s.state == :success
    end

    rp.write <+ (rr.write_requests * member).pairs {|l,r| [l.request, r.host, l.key, l.v_vector, l.value]}
  end

  # write logic
  bloom do
    # if we get a put, query AT LEAST # required ack servers for vector versions
    get_version <= put {|p| [p.request, p.key]}
    
    # if version_query was successful produce the latest version_vector
    vvm.version_matrix <= (version_acks * result).pairs(:request=>:ballot_id) do |l,r|
      [l.request, l.v_vector] if r.status == :success
    end

    # increment self in merged vector clock
    vvs.deserialize <= vvm.merge_vector

    # increment if element is a coordinator
    incremented_coordinators <= (vvs.deserialize_ack * my_addr).pairs do |l,r|
      [l.request, l.server, l.version + 1] if l.server == r.host
    end

    # element stays the same if element is not a coordinator
    incremented_coordinators <= (vvs.deserialize_ack * my_addr).pairs do |l,r|
      l if l.server != r.host
    end

    # reserialize the incremented vectors
    vvs.serialize <= incremented_coordinators

    # create the write request on the next timestep, or else we'll get a key error in voting from the finishing get_versions request on this timestep
    ready_puts <+ (pending_puts * vvs.serialize_ack).matches do |l,r|
      [l.request, l.key, r.v_vector, l.value]
    end
  end

=begin
  #debug
  bloom do
stdio <~ alarm.stop_alarm {|t| ["stop_alarm contains "+t.inspect+" at #{budtime}"]}
    stdio <~ ready_puts {|t| ["ready_puts contains "+t.inspect+" at #{budtime}"]}
    stdio <~ write_acks {|t| ["write_acks contains "+t.inspect+" at #{budtime}"]}
    stdio <~ status {|t| ["status contains "+t.inspect+" at #{budtime}"]}
    stdio <~ version_acks {|t| ["version_acks contains "+t.inspect+" at #{budtime}"]}
    stdio <~ result {|t| ["result contains "+t.inspect+" at #{budtime}"]}
    stdio <~ alarm.alarm {|t| ["alarm contains "+t.inspect+" at #{budtime}"]}
    stdio <~ alarm.countdowns {|t| ["countdowns contains "+t.inspect+" at #{budtime}"]}
    stdio <~ num_required {|t| ["num_required contains "+t.inspect+" at #{budtime}"]}
    stdio <~ pending_puts {|t| ["pending_puts contains "+t.inspect+" at #{budtime}"]}
    stdio <~ read_acks {|t| ["read_acks contains "+t.inspect+" at #{budtime}"]}
    stdio <~ get_cache {|t| ["get_cache contains "+t.inspect+" at #{budtime}"]}
  end
=end

end

# This module can be used by a client module to perform read and write
# requests with session guarantees.
# TODO consider letting this take care of version ack responses.
module SessionQuorumKVS
  include SessionQuorumKVSProtocol
  import RWTimeoutQuorumAgentProtocol => :quorum_agent
  import SessionVoteCounter => :session_manager

  bloom :read_request do
    # Initialize request in session_manager.
    session_manager.init_request <= kvread do |req|
      [req.reqid, req.session_types, req.vector, req.write_vector]
    end
    quorum_agent.get <= kvread {|read| [read.reqid, read.key] }
  end

  bloom :write_response do
    # Initialize request in session_manager.
    session_manager.init_request <= kvwrite do |req|
      [req.reqid, req.session_types, req.vector, req.write_vector]
    end
    quorum_agent.put <= kvwrite {|write| [write.reqid, write.key, write.value] }
  end

  bloom :handle_get_responses do
    session_manager.add_read <= quorum_agent.get_responses do |resp|
      [resp.request, resp.v_vector, resp.value]
    end
  end

  bloom :handle_version_responses do
    session_manager.add_write <= quorum_agent.version_responses do |resp|
      [resp.request, resp.v_vector]
    end
  end

  bloom :output_results do
    kvread_response <= session_manager.output_read_result
    kvwrite_response <= session_manager.output_write_result
  end

  bloom :end_request do
    session_manager.end_request <= quorum_agent.status do |stat|
      [stat.request] if stat.state == :sucess or stat.state == :fail
    end
  end

end

=begin
# write test
a = RWTimeoutQuorumAgent.new(:ip=>'127.0.0.1',:port=>'9007')
a.add_member <+ [['127.0.0.1:9007', 0],['127.0.0.1:9008', 1]]
a.my_id <+ [[0]]
a.put <+ [[1, "key", ["value"]]]
a.parameters <+ [[1, 1, 20]]
20.times {a.tick}
a.get <+ [[2, "key"]]
a.parameters <+ [[2, 1, 20]]
20.times {a.tick}
=end

# get version test
=begin
a = RWTimeoutQuorumAgent.new(:ip=>'127.0.0.1',:port=>'9007')
a.add_member <+ [['127.0.0.1:9007', 0],['127.0.0.1:9008', 1]]
a.my_id <+ [[0]]
a.get_version <+ [[1, "key"]]
a.parameters <+ [[1, 1, 20]]
10.times {a.tick}
=end
