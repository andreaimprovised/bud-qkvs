require 'rubygems'
require 'bud'
require 'version_vector'

# How to use: (anything that appears in an input interface, appears in the output interface in one tick)
#
# When the quorum agent has received all of the required read acks, it should pipe those acks into read_acks and the corresponding read request in read_requests
# This implementation allows several read requests to be handled on the same timestep
#
# Write requests will contain only one write per request id if the input read_acks were determined to be concurrent, or if any of the readacks contained stale
# data. If there were no concurrent readacks, and none of the reads were stale, then nothing will appear in the write_requests output interface
#
module ReadRepairProtocol
  state do
    interface input, :read_acks, [:request, :v_vector] => [:value]
    interface input, :read_requests, [:request] => [:key]
    
    # if acks are not synchronized, write requests will be non-empty. The contents of write requests should be piped into the corresponding write channel
    interface output, :write_requests, [:request] => [:key, :v_vector, :value]
  end
end

module ReadRepair
  include ReadRepairProtocol
  import VersionVectorConcurrency => :vvc
  import VersionVectorMerge => :vvm
  
  state do
    scratch :freshest_reads, [:request, :v_vector] => [:value]
    scratch :merged_values, [:request] => [:value]
    scratch :num_freshest_reads, [:request] => [:size]
    scratch :pending_write_requests, [:request] => [:key, :v_vector, :value]
  end

  bloom :determine_concurrent_clocks do
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

    # pending_write_requests contains one write per request id, that is guaranteed to be fresh. That write might be redundant, however
    pending_write_requests <= (read_requests * merged_values * vvm.merge_vector).matches do |l,m,r|
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
  
