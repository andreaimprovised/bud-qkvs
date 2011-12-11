require 'rubygems'
require 'bud'
require 'backports'

# Serializes and Deserializes vector clocks.
#
# Outputs are returned in the same timestep and do not persist.
module VersionVectorSerializerProtocol
  state do
    # Serialization
    interface input, :serialize, [:request, :server] => [:version]
    interface output, :serialize_ack, [:request] => [:v_vector]
    # Deserialization
    interface input, :deserialize, [:request] => [:v_vector]
    interface output, :deserialize_ack, [:request, :server] => [:version]
  end
end

module VersionVectorSerializer
  include VersionVectorSerializerProtocol

  bloom do
    serialize_ack <= serialize.reduce({}) do |meta, x|
      meta[x.request] ||= []
      meta[x.request] << [x.server, x.version]
      meta
    end
    deserialize_ack <= deserialize.flat_map do |x|
      x.v_vector.map do |y|
        [x.request, y[0], y[1]]
      end
    end
  end
end

# Given a matrix of version vectors, this module will produce a version
# vector containing the per-element max of all vectors, aka a vector that
# dominates all the given version vectors.
#
# Outputs are returned same timestep and do not persist.
module VersionVectorMergeProtocol
  state do
    interface input, :version_matrix, [:request, :v_vector] => []
    interface output, :merge_vector, [:request] => [:v_vector]
  end
end

module VersionVectorMerge
  include VersionVectorMergeProtocol
  import VersionVectorSerializer => :vvs

  state do
    scratch :deserialized_matrix, \
    [:request, :v_vector, :server] => [:version]
  end

  bloom do
    vvs.deserialize <= version_matrix do |x|
      [x, x.v_vector]
    end
    deserialized_matrix <= vvs.deserialize_ack do |x|
      [x.request[0], x.request[1], x.server, x.version]
    end
    vvs.serialize <= deserialized_matrix\
      .group([:request, :server], max(:version))
    merge_vector <= vvs.serialize_ack
  end

end

# Given two verion vectors, this module computes the order between the
# two version vectors.
#
# Behavior:
# If v1 --> v2, then order = -1
# If v1 <--> v2, then order = 0
# If v1 <-- v2, then order = 1
module VersionVectorDominationProtocol
  state do
    interface input, :domination_query, [:request] => [:v1, :v2]
    interface output, :result, [:request] => [:order]
  end
end

module VersionVectorDomination
  include VersionVectorDominationProtocol
  import VersionVectorSerializer => :vvs

  state do
    scratch :less, [:request] => []
    scratch :more, [:request] => []

    scratch :deserialized_v1, [:request, :server] => [:version]
    scratch :deserialized_v2, [:request, :server] => [:version]
  end

  bloom do
    vvs.deserialize <= domination_query.flat_map do |x|
      [[[x.request, 1], x.v1], [[x.request, 2], x.v2]]
    end

    deserialized_v1 <= vvs.deserialize_ack do |x|
      [x.request[0], x.server, x.version] if x.request[1] == 1
    end
    deserialized_v2 <= vvs.deserialize_ack do |x|
      [x.request[0], x.server, x.version] if x.request[1] == 2
    end

    less <= (deserialized_v1 * deserialized_v2)\
      .outer(:request => :request, :server => :server) do |v1, v2|
      [v1.request] if v1.version < Integer(v2.version)
    end
    less <= (deserialized_v2 * deserialized_v1)\
      .outer(:request => :request, :server => :server) do |v2, v1|
      [v2.request] if Integer(v1.version) < v2.version
    end
    
    more <= (deserialized_v1 * deserialized_v2)\
      .outer(:request => :request, :server => :server) do |v1, v2|
      [v1.request] if v1.version > Integer(v2.version)
    end
    more <= (deserialized_v2 * deserialized_v1)\
      .outer(:request => :request, :server => :server) do |v2, v1|
      [v2.request] if Integer(v1.version) > v2.version
    end

    temp :more_order <= more.notin(less, :request => :request)
    temp :less_order <= less.notin(more, :request => :request)

    result <= (more * less).lefts(:request => :request) do |x|
      [x.request, 0]
    end
    result <=  domination_query do |x|
      [x.request, 1] if !less.include? [x.request] \
                     and !more.include? [x.request]
    end
    result <= less_order { |x| [x.request, -1] }
    result <= more_order { |x| [x.request, 1] }

  end
end


# Given a matrix (or list) of version vectors, this module computes
# the single latest version vector for all concurrent lines.
#
# Output is returned in the same timestep and does not persist.
module VersionVectorConcurrencyProtocol
  state do
    interface input, :version_matrix, [:request, :v_vector] => []
    interface output, :minimal_matrix, [:request, :v_vector] => []
    interface output, :obselete_matrix, [:request, :v_vector] => []
  end
end

module VersionVectorConcurrency
  include VersionVectorConcurrencyProtocol
  import VersionVectorDomination => :vvd

  bloom do
    vvd.domination_query <= (version_matrix * version_matrix)\
      .pairs(:request => :request) do |v1, v2|
      [[v1.request, v1.v_vector, v2.v_vector], v1.v_vector, v2.v_vector] 
    end
    obselete_matrix <= vvd.result do |x|
      [x.request[0], x.request[1]] if x.order == -1
    end
    minimal_matrix <= version_matrix.notin(obselete_matrix, \
                                           :request => :request,\
                                           :v_vector => :v_vector)
  end
  
end

# Returns a same timestep acknowledgement of the read.  Returns the 
# ack for the write in the next timestep.
# 
# If you perform a read an nothing show up in read_ack, you can safely
# conluce that the KVS does not contain that key.
#
# When you perform a write, the write ack contains the new list of
# (vector, value) pairs that are in the KVS the next timestep.
#
# The user of this module must specify the version vector for a write.
# The module will take care of properly doing an update, by keeping
# track of concurrent version and also replacing versions which the write
# dominates.
#
# To determine the version vector to write, you must either consult the
# quorum, or read the current version, or have the vector cached in a
# session, or etc...
#
# Outputs do not persist across timesteps.
module VersionVectorKVSProtocol
  state do
    # Read operations
    interface input, :read, [:request] => [:key]
    interface output, :read_ack, [:request, :v_vector] => [:value]
    # Write operations
    interface input, :write, [:request] => [:key, :v_vector, :value]
    interface output, :write_ack, [:request, :v_vector] => [:value]
  end
end

module VersionVectorKVS
  include VersionVectorKVSProtocol
  import VersionVectorConcurrency => :vvc

  state do
    table :kv_store, [:key, :v_vector] => [:value]
    scratch :write_to_ack, [:request] => [:key]
  end

  # Handle Read Requests
  bloom :read do
    read_ack <= (read * kv_store).pairs(:key => :key) do |r,s|
      [r.request, s.v_vector, s.value]
    end
  end
  
  # Hanlde Write Requests
  bloom :write do
    # Compute minimal and obselete entries
    vvc.version_matrix <= (write * kv_store)\
      .lefts(:key => :key) do |w,s|
      [w.key, s.v_vector]
    end
    vvc.version_matrix <= write do |w|
      [w.key, w.v_vector]
    end

    # Update store
    kv_store <- (kv_store * vvc.obselete_matrix)\
      .lefts(:key => :request, :v_vector => :v_vector)
    kv_store <+ (vvc.minimal_matrix * write)\
      .rights(:v_vector => :v_vector, :request => :key) do |w|
      [w.key, w.v_vector, w.value]
    end

    # Ack the write
    write_to_ack <+ write { |w| [w.request, w.key] }
    write_ack <= (write_to_ack * kv_store)\
      .pairs(:key => :key) do |a,s|
      [a.request, s.v_vector, s.value]
    end
  end
end

