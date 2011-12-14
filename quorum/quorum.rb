require 'rubygems'
require 'bud'
require 'quorum/membership'
require 'causality/version_vector'


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

# Performs read/version/write operations on static members
#
# Doesn't know about session guarantees 
#
# Write operation must specifiy version vector.  No logic for
# specifying the proper version vector.
#
# acks are asynchronous streams
module QuorumRemoteProcedureProtocol

  # Request must be unique across all read/version/write operations
  # Agent is a network identifier

  state do
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
    channel :read_request, [:@dst, :src, :request] => [:key]
    channel :read_response, [:dst, :@src, :request] => [:matrix]
    channel :version_request, [:@dst, :src, :request] => [:key]
    channel :version_response, [:dst, :@src, :request] => [:v_matrix]
    channel :write_request, \
      [:@dst, :src, :request] => [:key, :v_vector, :value]
    channel :write_response, [:dst, :@src, :request] => []
  end
  
  # Send out read operation request and ack
  bloom do
    read_request <~ read do |r|
      [r.agent, ip_port, r.request, r.key]
    end
    vvms.deserialize <= read_response do |r|
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
    vvms.serialize <= vvkvs.read_ack
    read_response <~ vvms.serialize_ack do |a|
      [ip_port, a.request[0], a.request[1], a.matrix]
    end
  end

  # Send out version operation request and ack
  bloom do
    version_request <~ version_query do |x|
      [x.agent, ip_port, x.request, x.key]
    end
    vms.deserialize <= version_response do |x|
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
    vms.serialize <= vvkvs.version_ack
    version_response <~ vms.serialize_ack do |a|
      [ip_port, a.request[0], a.request[1], a.v_matrix]
    end
  end

  # Send out write operation requests from members and ack
  bloom do
    write_request <~ write do |w|
      [w.agent, ip_port, w.request, w.key, w.v_vector, w.value]
    end
    write_ack <= write_response do |w|
      [w.request, w.dst]
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

