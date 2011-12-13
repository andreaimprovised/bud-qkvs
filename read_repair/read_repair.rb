require 'rubygems'
require 'bud'
require 'causality/version_vector'

# How to use: (anything that appears in an input interface, appears in the output interface in one tick. NO STATE PERSISTS WITHIN THIS MODULE)
#
# When the quorum agent has received all of the required read acks, it should pipe those acks into read_acks and the corresponding read request in read_requests
# This implementation allows several read requests to be handled on the same timestep
#
# Write requests will contain only one write per request id if the input read_acks were determined to be concurrent, or if any of the readacks contained stale
# data. If there were no concurrent readacks, and none of the reads were stale, then nothing will appear in the write_requests output interface
#
module ReadRepairProtocol
  state do
    # NOTE: A read can result in the same readacks among all reads. This corresponds to a read in whic all of the versions are the same
    # The user is responsible for ensuring that only one instance of duplicate read_acks is inserted into this interface
    interface input, :read_acks, [:request, :v_vector] => [:value]
    
    # NOTE: read_requests DOES NOT PERSIST INTERNALLY. It is up to the user of this module to cache the key, and coordinator server for a read request.
    # The user can garbage collect the cached state after it has received all the required read acks and piped it into this interface
    interface input, :read_requests, [:request] => [:key, :server]
    
    # NOTE: this module only returns a single write per request if version conflicts were detected. The quorum layer is responsible for propagating the
    # write request to the proper nodes
    interface output, :write_requests, [:request] => [:key, :v_vector, :value]
  end
end

module ReadRepair
  include ReadRepairProtocol
  include VersionVectorSerializerProtocol
  import VersionVectorConcurrency => :vvc
  import VersionVectorMerge => :vvm
  
  state do
    # temp state. I wish temps could have schema declarations
    scratch :freshest_reads, [:request, :v_vector] => [:value]
    scratch :merged_values, [:request] => [:value]
    scratch :num_freshest_reads, [:request] => [:size]
    scratch :incremented_coordinators, [:request, :server] => [:version]
    scratch :pending_write_requests, [:request] => [:key, :v_vector, :value]
  end

  bloom :determine_concurrent_clocks do
    # some glue code
    vvc.version_matrix <= read_acks { |ra| [ra.request, ra.v_vector] }
  end

  bloom :read_repair do
    # remove stale reads from the ack set
    freshest_reads <= (vvc.minimal_matrix * read_acks).rights(:request => :request, :v_vector => :v_vector)

    # combine the values of minimal matrix
    merged_values <= freshest_reads.reduce({}) do |memo, t|
      memo[t.request] ||= []
      memo[t.request] |= t.value
      memo
    end

    # merge the clocks of the minimal matrix
    vvm.version_matrix <= freshest_reads {|t| [t.request, t.v_vector]}

    # increment the coordinator in the merged vector clock
    # is another VV serializer implementation, but this one increments coordinataors
    deserialize <= vvm.merge_vector
    deserialize_ack <= deserialize.flat_map do |x|
      x.v_vector.map do |y|
        [x.request, y[0], y[1]]
      end
    end
    
    # increment if element is a coordinator
    incremented_coordinators <= (deserialize_ack * read_requests).pairs(:request=>:request) do |l,r| 
      [l.request, l.server, l.version + 1] if l.server == r.server
    end

    # element stays the same if it is not a coordinator
    incremented_coordinators <= (deserialize_ack * read_requests).pairs(:request=>:request) do |l,r|
      l if l.server != r.server
    end
    
    # reserialize the incremented vectors
    serialize <= incremented_coordinators

    serialize_ack <= serialize.reduce({}) do |meta, x|
      meta[x.request] ||= []
      meta[x.request] << [x.server, x.version]
      meta
    end

    # pending_write_requests contains one write per request id, that is guaranteed to be fresh. That write might be redundant, however
    pending_write_requests <= (read_requests * merged_values * serialize_ack).matches do |l,m,r|
      [l.request, l.key, r.v_vector, m.value]
    end

    # only do read repair if a version got dominated, or there are multiple concurrent, fresh reads
    write_requests <= (pending_write_requests * vvc.obselete_matrix).lefts(:request=>:request)

    num_freshest_reads <= freshest_reads.reduce({}) do |memo, t|
      memo[t.request] ||=0
      memo[t.request] += 1
      memo
    end

    write_requests <= (pending_write_requests * num_freshest_reads).matches do |l,r|
      l if r.size > 1
    end
  end
end
  
