require 'rubygems'
require 'bud'
require 'quorum/membership'
require 'causality/version_vector'
 
# Performs read/version/write operations on static members
#
# Doesn't know about session guarantees 
#
# Write operation must specifiy version vector.  No logic for
# specifying the proper version vector.
#
# acks are asynchronous streams
module QuorumAgentProtocol
  include MembershipProtocol

  state do
    # Read Operation
    interface input, :read, [:request] => [:key]
    interface output, :read_ack,  [:request, :v_vector] => [:value]
    
    # Version Query
    interface input, :version_query, [:request] => [:key]
    interface output, :version_ack, [:request, :v_vector] => []
    
    # Write Operation
    interface input, :write, [:request] => [:key, :v_vector, :value]
    interface output, :write_ack, [:request] => []
  end
end

# Serializes several (vector, value) pairs into one field
module VectorValueMatrixSerializerProtocol
  state do
    # Serialize
    interface input, :serialize, [:request, :v_vector] => [:value]
    interface output, :serialize_ack, [:request] => [:matrix]
    # Deserialize
    interface input, :deserialize, [:request] => [:matrix]
    interface input, :deserialize_ack, [:request, :v_vector] => [:value]
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

module QuorumAgent
  include QuorumAgentProtocol
  include StaticMembership
  import VersionVectorKVS => :vvkvs
  import VersionMatrixSerializer => :vms
  import VectorValueMatrixSerializer => :vvms

  state do
    channel :read_request, [:@dst, :src, :request] => [:key]
    channel :read_response, [:dst, :@src, :request] => [:matrix]
    channel :version_request, [:@dst, :src, :request] => [:key]
    channel :version_response, [:dst, :@src, :request] => [:matrix]
    channel :write_request, \
      [:@dst, :src, :request] => [:key, :v_vector, :value]
    channel :write_response, [:dst, :@src, :request] => []
  end
  
  # Send out read operation requests to members and ack
  bloom do
    read_request <~ (member * read).pairs do |m, r|
      [m.host, ip_port, r.request, r.key]
    end
    vvms.deserialize <= read_response do |r|
      [r.request, r.matrix]
    end
    read_ack <= vvms.deserialize_ack
    end
  end

  # Reply to read operation requests to members
  bloom do
    vvkvs.read <= read_request do |r|
      [[r.src, r.request], r.key]
    end
    read_response <~ vvkvs.read_ack do |a|
      [ip_port, a.request[0], a.request[1], a.v_vector, a.value]
    end
  end

  # Send out version operation requests to members and ack
  bloom do
    version_request <~ (member * version_query).pairs do |m, v|
      [m.host, ip_port, v.request, v.key]
    end
    version_ack <= version_response do |v|
      [v.request, v.v_vector]
    end
  end

  # Reply to version operation requests to members
  bloom do
    vvkvs.version_query <= version_request do |v|
      [[v.src, v.request], v.key]
    end
    version_response <~ vvkvs.version_ack do |a|
      [ip_port, a.request[0], a.request[1], a.v_vector]
    end
  end
  
  # Send out write operation requests from members and ack
  bloom do
    write_request <~ (member * write).pairs do |m, w|
      [m.host, ip_port, w.request, w.key, w.v_vector, w.value]
    end
    write_ack <= write_response do |w|
      [w.request]
    end
  end

  # Reply to write operation requests from members
  bloom do
    vvkvs.write <= write_request do |w|
      [[w.src, w.request], w.key, w.v_vector, w.value]
    end
    write_response <~ vvkvs.write_ack do |a|
      [ip_port, a.request[0], a.request[1]]
    end
  end

end

module RWTimeoutQuorumAgentProtocol
  include QuorumAgentProtocol
  
  # interface input, :begin_vote, [:ballot_id] => [:num_votes]
  # interface input, :cast_vote, [:ballot_id, :agent, :vote, :note]
  # interface output, :result, [:ballot_id] => [:status, :result,
  #                                             :votes, :notes

  # interface input, :set_alarm, [:ident] => [:duration]
  # interface input, :stop_alarm, [:ident] => []
  # interface output, :alarm, [:ident] => []

  state do
    # Parameter Input and Status Output
    # ack_size is the number of acks to wait for to declare victory
    # duration is the time in units of 0.1s to wait until failure
    interface input, :parameters, [:request] => [:ack_num, :duration]
    interface input, :delete, [:request] => []
    interface output, :status, [:request] => [:state]   
  end

end

module RWTimeoutQuorumAgent
  import Alarm => :alarm
  import VoteCounter => :voter
  import QuorumAgent => :qa

  state do
    table :read_acks, [:request, :v_vector] => [:
  end

  # Begin vote and set alarm
  bloom do
    voter.begin_vote <= (read * parameters)\
      .pairs(:request => :request) do |x,p| 
      [x.request, p.ack_num]
    end
    voter.begin_vote <= (version_query * parameters)\
      .pairs(:request => :request) do |x,p| 
      [x.request, p.ack_num]
    end
    voter.begin_vote <= (write * parameters)\
      .pairs(:request => :request) do |x,p| 
      [x.request, p.ack_num]
    end
    alarm.set_alarm <= (read * parameters)\
      .pairs(:request => :request) do |x,p|
      [x.request, p.duration]
    end
    alarm.set_alarm <= (version_query * parameters)\
      .pairs(:request => :request) do |x,p|
      [x.request, p.duration]
    end
    alarm.set_alarm <= (write * parameters)\
      .pairs(:request => :request) do |x,p|
      [x.request, p.duration]
    end
  end
  
  # record votes!
  bloom do
    voter.cast_vote <= read_ack do |
  end
end
