require 'rubygems'
require 'bud'

# This module will be used on a QuorumKVS as a voting module to pass
# results to. Results that satisfy the required guarantees are output
# as satisfiers.
module SessionVoteCounterProtocol
  interface input, :init_request, [:reqid, :session_types, :read_vectors, \
    :write_vector]
  interface input, :vote_read, [:v_vector, :reqid] => [:value]
  interface input, :vote_write, [:v_vector, :reqid] => []
  interface input, :clear_request, [:reqid] => []
  interface output, :output_read_result, [:reqid, :v_vector] => [:value]
  interface output, :output_write_result, [:reqid] => [:v_vector]

  # TODO add some interface to initialize a vote
  # (add reqid => {read,write}vector.)
end

module SessionVoteCounter
  include SessionVoteCounterProtocol
  require VersionVectorMerge => :merge_vectors
  require VersionVectorConcurrency => :aggregate_vectors
  require VersionMatrixSerializer => :serialize_matrix

  state do
    table :read_vectors, [:reqid, :vector] => []
    table :write_vector, [:reqid] => [:vector]
    # reqid => list of guarantees.
    table :session_guarantees, [:reqid, :session_types]

    table :pending_reads, vote_read.schema
    table :pending_writes, vote_write.schema
  end

  bloom :init_session do
    session_guarantees <= init_request {|init| [init.reqid, init.session_types]}
    # Set read vectors.
    serialize_matrix.deserialize <= init_request {|init| init.read_vectors }
    read_vector <= serialize_matrix.deserialize_ack
    # Set write vector.
    write_vector <= init_request {|init| init.write_vector }
  end

  bloom :aggregate_read_votes do
    pending_reads <= vote_read
    aggregate_vectors.version_matrix <= pending_reads {|r| [r.reqid, r.v_vector]}
  end

  bloom :handle_read_session_guarantees do
    # Add read vectors to aggregate for monotonic reads.
    aggregate_vectors.version_matrix <= (session_guarantees * read_vectors).pairs(
      :reqid => :reqid) do |guarantees, vector|
      vector if guarantees.session_types.include? :MR
    end
    # Add write vectors to aggregate for read your writes.
    aggregate_vectors.version_matrix <= (session_guarantees * write_vectors).pairs(
      :reqid => :reqid) do |guarantees, vector|
      vector if guarantees.session_types.include? :RYW
    end
  end

  bloom :merge_write_votes do
    pending_writes <= vote_write
    merge_vectors.version_matrix <= vote_write {|w| [w.reqid, w.v_vector]}
  end

  bloom :handle_write_session_guarantees do
    # Add read vectors to merge for writes follow reads.
    merge_vectors.version_matrix <= (session_guarantees * read_vectors).pairs(
      :reqid => :reqid) do |guarantees, vector|
      vector if guarantees.session_types.include? :WFR
    end
    # Add write vectors to merge for read your writes.
    merge_vectors.version_matrix <= (session_guarantees * write_vectors).pairs(
      :reqid => :reqid) do |guarantees, vector|
      vector if guarantees.session_types.include? :MW
    end
  end

  bloom :output_read_results do
    output_read_result <= (aggregate_vectors.minimal_matrix * pending_reads).pairs(
      :reqid => :reqid, :v_vector => :v_vector) do |min,reads|
      [min.reqid, min.v_vector, reads.value]
    end
    output_read_result <= (aggregate_vectors.minimal_matrix * read_vector).pairs(
      :reqid => :reqid) do |min,read_vec|
      [min.reqid, read_vec.v_vector, nil]
    end
  end

  bloom :output_write_results do
    output_write_result <= merge_vectors.merge_vector
  end

  # Remove requests that have been cleared.
  bloom :cleanup_results do
    read_vectors <+ read_vectors.notin(clear_requests, :reqid => :reqid)
    write_vectors <+ write_vectors.notin(clear_requests, :reqid => :reqid)
    session_guarantees <+ session_guarantees.notin(clear_requests, :reqid => :reqid)
    pending_reads <+ pending_reads.notin(clear_requests, :reqid => :reqid)
    pending_writes <+ pending_writes.notin(clear_requests, :reqid => :reqid)
  end

end
