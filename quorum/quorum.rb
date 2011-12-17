require 'rubygems'
require 'bud'
require 'membership/membership'
require 'causality/version_vector'
require 'vote/voting'
require 'alarm/alarm'
require 'read_repair/read_repair'
require 'session_guarantees/session_guarantee_voting'
require 'session_guarantees/session_guarantee_client'


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

  state do
    table :pending_request, [:request] => []
    table :pending_response, [:request] => []

    channel :read_request, [:@dst, :src, :request] => [:key]
    channel :read_response, [:dst, :@src, :request] => [:matrix]
    channel :version_request, [:@dst, :src, :request] => [:key]
    channel :version_response, [:dst, :@src, :request] => [:v_matrix]
    channel :write_request, \
      [:@dst, :src, :request] => [:key, :v_vector, :value]
    channel :write_response, [:dst, :@src, :request] => []
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

# FOR GETS AND GET_VERSION: get(request,key)/get_version(request,key) will output the partial set of required gets so far if status is outputting :in progress. If status outputs fail, then the request timed out. If it outputs success, the output channels will have AT LEAST the number of required acks the client specified.

# PUTS ARE A LITTLE WEIRD TO USE: After a put(key,val) is invoked, a get_version using the same parameters specified is issued under the hood. A successful put corresponds to seeing status :success on a single request in 2 different timesteps.
# Status :fail after a write means that your put request timed out while retrieving at least the specified number of versions for your key.
# Status :success, Status :fail after a write means that your put request successfully attempted to write to at least the specified number of servers, but it didn't hear back from enough servers

# BIG ASS CAVEAT: The writers of this module assumed that if server, call it Src sends 2 status updates over a channel on 2 different timesteps, then the client, call it dst, will receive the status updates on 2 different timesteps.
# Stuff will break if this assumption is not respected since put requests are split into 2 different operations (get versions, and send with updated version). These 2 operations use the same identifier, so if dst receives the 2 status updates in its channel, on the same timestep, a key error will result.

module RWTimeoutQuorumAgent
  include RWTimeoutQuorumAgentProtocol
  include StaticMembership
  import Alarm => :alarm
  import CountVoteCounter => :voter
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
    table :self_addr, [] => [:host]
    table :put_parameters, [:request] => [:ack_num, :duration]
    
    # for incrementing coordinator node in a write
    scratch :incremented_coordinators, [:request, :server] => [:version]
  end


  # MISC Logic block
  state do
    # cache puts if we are waiting for versions to be read
    pending_puts <= put
    # cache put parameters since we'll need it to be around for the second tick if we write
    put_parameters <= (pending_puts * parameters).pairs(:request=>:request) {|l,r| r}
    # remember local address
    self_addr <= (local_id * member).pairs(:ident=>:ident) {|l,r| r.host}
    # cache gets for read repair
    get_cache <= get
  end

  # setup num candidates, set voting paramters, begin vote and set alarm
  bloom do
    # setup num expected voters
    voter.begin_vote <= get {|g| [g.request, 0xffffffff]}
    voter.begin_vote <= get_version {|g| [g.request, 0xffffffff]}
    voter.begin_vote <= ready_puts {|p| [p.request, 0xffffffff]}
    voter.num_required <= (get * parameters)\
      .pairs(:request => :request) do |x,p| 
      [x.request, p.ack_num]
    end
    voter.num_required <= (get_version * parameters)\
      .pairs(:request => :request) do |x,p| 
      [x.request, p.ack_num]
    end
    voter.num_required <= (ready_puts * parameters)\
      .pairs(:request => :request) do |x,p| 
      [x.request, p.ack_num]
    end
    alarm.set_alarm <= (get * parameters)\
      .pairs(:request => :request) do |x,p|
      [x.request, p.duration]
    end
    alarm.set_alarm <= (get_version * parameters)\
      .pairs(:request => :request) do |x,p|
      [x.request, p.duration]
    end
    alarm.set_alarm <= (ready_puts * parameters)\
      .pairs(:request => :request) do |x,p|
      [x.request, p.duration]
    end
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
    voter.cast_vote <= rp.read_ack {|rr| [rr.request, rr.src, "ack", "read ack"]}
    voter.cast_vote <= rp.version_ack {|vr| [vr.request, vr.src, "ack", "version ack"]}
    voter.cast_vote <= rp.write_ack {|wr| [wr.request, wr.src, "ack", "write ack"]}
    # cache remote procedure return values
    read_acks <= rp.read_ack
    version_acks <= rp.version_ack
    write_acks <= rp.write_ack
  end

  # handling results and timeouts
  bloom do
     # timer runs out, vote fails, output error
    status <= (alarm.alarm * voter.result).pairs(:ident=>:ballot_id) do |l,r|
      [l.ident, :fail] if r.status == :fail
    end
    # timer runs out, vote succeeds, output success
    status <= (alarm.alarm * voter.result).pairs(:ident=>:ballot_id) do |l,r|
      [l.ident, :success] if r.status == :success
    end

    # vote successful, timer still going, clear timer: output success
    status <= voter.result do |r|
      [r.ballot_id, :success] if r.status == :success
    end
    
    alarm.stop_alarm <= voter.result do |r|
      [r.ballot_id] if r.status == :success 
    end

    # clear cached data
    pending_puts <- (pending_puts * voter.result).pairs(:request=>:ballot_id) do |l,r|
      l if r.status == :success or r.status == :fail
    end
    ready_puts <- (ready_puts * voter.result).pairs(:request=>:ballot_id) do |l,r|
      l if r.status == :success or r.status == :fail
    end

    read_acks <- (read_acks * voter.result).pairs(:request=>:ballot_id) do |l,r|
      l if r.status == :success or r.status == :fail
    end

    version_acks <- (version_acks * voter.result).pairs(:request=>:ballot_id) do |l,r|
      l if r.status == :success or r.status == :fail
    end
    write_acks <- (write_acks * voter.result).pairs(:request=>:ballot_id) do |l,r|
      l if r.status == :success or r.status == :fail
    end
    get_cache <- (get_cache * voter.result).pairs(:request=>:ballot_id) do |l,r|
      l if r.status == :success or r.status == :fail
    end
    # clear put_parameters if we couldn't get enough version acks
    put_parameters <- (version_acks * voter.result * put_parameters).combos(version_acks.request=>voter.result.ballot_id, version_acks.request => put_parameters.request) do |l,m,r|
      r if m.status == :fail
    end
    # clear put_parameters if write done
    put_parameters <- (write_acks * voter.result * put_parameters).combos(write_acks.request=>voter.result.ballot_id, write_acks.request => put_parameters.request) do |l,m,r|
      r if m.status == :fail or m.status == :success
    end
  end

  # handle output
  bloom do
    #get_responses <= read_acks
    put_responses <= write_acks
    version_responses <= version_acks

    # do read_repair silently

    rr.read_acks <= read_acks {|r| [r.request, r.v_vector, r.value]}

    rr.read_requests <= (read_acks * get_cache * self_addr).combos(read_acks.request=>get_cache.request) do |l,m,r| 
      [l.request, m.key, r.host]
    end

    rp.write <+ (rr.write_requests * member).pairs {|l,r| [l.request, r.host, l.key, l.v_vector, l.value]}
  end

  # write logic
  bloom do
    # if we get a put, query AT LEAST # required ack servers for vector versions
    get_version <= put {|p| [p.request, p.key]}
    
    # if version_query was successful, specify the parameters for our write request, which will be issued on the next timestep. Must happen on the next timestep because the corresponding get_version request is successfully completing on this timestep
    parameters <+ (put_parameters * version_acks * voter.result).matches do |l,m,r|
      l if r.status == :success
    end

    # if version_query was successful produce the latest version_vector
    vvm.version_matrix <= (version_acks * voter.result).pairs(:request=>:ballot_id) do |l,r|
      [l.request. l.v_vector] if r.status == :success
    end
    
    # increment self in merged vector clock
    vvs.deserialize <= vvm.merge_vector
    
    # increment if element is a coordinator
    incremented_coordinators <= (vvs.deserialize_ack * self_addr).pairs do |l,r|
      [l.request, l.server, l.version + 1] if l.server == r.host
    end

    # element stays the same if element is not a coordinator
    incremented_coordinators <= (vvs.deserialize_ack * self_addr).pairs do |l,r|
      l if l.server == r.host
    end

    # reserialize the incremented vectors
    vvs.serialize <= incremented_coordinators

    # create the write request on the next timestep, or else we'll get a key error from the finishing get_versions request on this timestep
    ready_puts <+ (pending_puts * vvs.serialize_ack).matches do |l,r|
      [l.request, l.key, r.v_vector, l.value]
    end 
  end
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
