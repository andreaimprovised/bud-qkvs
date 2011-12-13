require 'rubygems'
require 'bud'
 
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
  # Read Operation
  interface input, :read, \
  [:request] => [:key]
  interface output, :read_ack,
  [:request, :v_vector] => [:value]

  # Version Query
  interface input, :version_query, \
  [:request] => [:key]
  interface output, :version_ack, \
  [:request, :v_vector] => []

  # Write Operation
  interface input, :write, \
  [:request] => [:key, :v_vector, :value]
  interface output, :write_ack, \
  [:request] => []
end

module QuorumAgent
  include StaticMembership
  import VersionVectorKVS => :vvkvs

  state do
    channel :read_request, [:@dst, :src, :request] => [:key]
    channel :read_response, [:dst, :@src, :request, :v_vector] => [:value]
    channel :version_request, [:@dst, :src, :request] => [:key]
    channel :version_response, [:dst, :@src, :request, :v_vector] => []
    channel :write_request, [:@dst, :src, :request] => [:key, :v_vector, :value]
    channel :write_response, [:dst, :@src, :request] => []
  end
  
  # Send out read operation requests to members and ack
  bloom do
    read_request <~ (member * read).pairs do |m, r|
      [m.host, ip_port, r.request, r.key]
    end
    read_ack <= read_response do |r|
      [r.request, r.v_vector, r.value]
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
    vvkvs.read <= version_request do |v|
      [[v.src, v.request], v.key]
    end
    version_response <~ vvkvs.read_ack do |a|
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
