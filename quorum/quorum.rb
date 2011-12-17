require 'rubygems'
require 'bud'
require 'membership/membership'
require 'causality/version_vector'
require 'vote/voting'
require 'alarm/alarm'


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
    interface input, :get, [:request] => [:key]
    interface input, :put, [:request] => [:key, :value]
    interface input, :get_version [:request] => [:key]

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

module RWTimeoutQuorumAgent
  include RWTimeoutQuorumAgentProtocol
  include StaticMembership
  import Alarm => :alarm
  import CountVoteCounter => :voter
  import QuorumRemoteProcedure => :rp

  state do
    table :read_acks [:request, :agent, :v_vector] => [:value]
    table :version_acks [:request, :agent, :v_vector] => []
    table :write_acks [:request, :agent]
    table :num_Agents [:host] => [:cnt]
    
    # cached puts that are still waiting for version queries
    table :pending_puts [:request] => [:key, :value]
    # puts that have an updated version vector, and synchronized values
    table :ready_puts [:request] => [:key, :v_vector, :value]
  end

  # MISC Logic block
  state do
    # count members
    num_Agents <= member.group([:host], count(:host))
    # cache put requests
    pending_puts <= put
  end

  # setup num candidates, set voting paramters, begin vote and set alarm
  bloom do
    voter.num_required <= (get * parameters)\
      .pairs(:request => :request) do |x,p| 
      [x.request, p.ack_num]
    end
    voter.num_required <= (get_version * parameters)\
      .pairs(:request => :request) do |x,p| 
      [x.request, p.ack_num]
    end
    voter.num_required <= (put * parameters)\
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
    alarm.set_alarm <= (put * parameters)\
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
    voter.cast_vote <= rp.read_response {|rr| [rr.request, rr.src, "ack", "read ack"]}
    voter.cast_vote <= rp.version_response {|vr| [vr.request, vr.src, "ack", "version ack"]}
    voter.cast_vote <= rp.write_response {|wr| [wr.request, wr.src, "ack", "write ack"]}
    # cache remote procedure return values
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
    
    alarm.stop_alarm <= vote.result do |r|
      [r.ballot_id] if r.status == :success
    end
    
  end
end

