require 'rubygems'
require 'bud'

# @abstract this module will be used in a QuorumKVS as a 'voting' module
# to pass read and write results to. When enough enough read or write
# results have been passed in to satisfy the required guarantees, the
# results are output. The output of this module is a streaming interface,
# so results will be output as they come in until the request is ended.
module SessionVoteCounterProtocol
  state do
    # Used to initialize a new read or write request.
    # @param [Object] reqid a unique id for the request
    # @param [List] session_types a list of guarantees to enforce
    #     within a session. Possible values:
    #     :MR - Reads have monotonically increasing versions
    #     :RYW - Reads versions that are >= those of prior writes
    #     :WFR - Writes version that are >= those of prior reads
    #     :MW - Writes versions that are >= those of prior writes
    # @param [List] read_vectors a list of vectors indicating the
    #     session's read set.
    # @param [VersionVector] write_vector a vector indicating the
    #     session's write set
    interface input, :init_request, [:reqid, :session_types, :read_vectors, \
      :write_vector]

    # Adds a 'vote' of a read result from a Quorum node.
    # @param [VersionVector] v_vector the version vector for the stored
    #     value
    # @param [Object] reqid a unique id for the request
    # @param [Object] value the value read from the KVS.
    interface input, :vote_read, [:v_vector, :reqid] => [:value]

    # Adds a 'vote' of a write result from a Quorum node.
    # @param [VersionVector] v_vector the version vector for the written
    #     value
    # @param [Object] reqid a unique id for the request
    interface input, :vote_write, [:v_vector, :reqid]

    # Signals to the module that the request is finished.
    # @param [Object] reqid the unique id of the request
    interface input, :end_request, [:reqid]

    # The stream of read results that satisfy all session guarantees.
    # There is either a single maximum result, or a number of maxmimal
    # results that occur concurrently.
    # @param [Object] reqid the unique id for the request
    # @param [VersionVector] the version vector for the stored value
    interface output, :output_read_result, [:reqid, :v_vector] => [:value]

    # The streamed write result that satisfies all session guarantees.
    # @param [Object] reqid the unique id for the request
    interface output, :output_write_result, [:reqid] => [:v_vector]
  end
end

module SessionVoteCounter
  include SessionVoteCounterProtocol
  require VersionVectorMerge => :vector_merger
  require VersionVectorConcurrency => :vector_aggregator
  require VersionMatrixSerializer => :matrix_serializer

  state do
    table :read_vectors, [:reqid, :vector]
    table :write_vector, [:reqid] => [:vector]
    # reqid => list of guarantees.
    table :session_guarantees, [:reqid, :session_types]

    table :pending_reads, vote_read.schema
    table :pending_writes, vote_write.schema
    scratch :read_results, output_read_result.schema
  end

  bloom :init_session do
    session_guarantees <= init_request {|init| [init.reqid, init.session_types]}
    # Set read vectors.
    matrix_serializer.deserialize <= init_request {|init| init.read_vectors }
    read_vector <= matrix_serializer.deserialize_ack
    # Set write vector.
    write_vector <= init_request {|init| init.write_vector }
  end

  bloom :aggregate_read_votes do
    pending_reads <= vote_read
    vector_aggregator.version_matrix <= pending_reads {|r| [r.reqid, r.v_vector]}
  end

  bloom :handle_read_session_guarantees do
    # Add read vectors to aggregate for monotonic reads.
    vector_aggregator.version_matrix <= (session_guarantees * read_vectors).pairs(
      :reqid => :reqid) do |guarantees, vector|
      vector if guarantees.session_types.include? :MR
    end
    # Add write vectors to aggregate for read your writes.
    vector_aggregator.version_matrix <= (session_guarantees * write_vectors).pairs(
      :reqid => :reqid) do |guarantees, vector|
      vector if guarantees.session_types.include? :RYW
    end
  end

  bloom :merge_write_votes do
    pending_writes <= vote_write
    vector_merger.version_matrix <= vote_write {|w| [w.reqid, w.v_vector]}
  end

  bloom :handle_write_session_guarantees do
    # Add read vectors to merge for writes follow reads.
    vector_merger.version_matrix <= (session_guarantees * read_vectors).pairs(
      :reqid => :reqid) do |guarantees, vector|
      vector if guarantees.session_types.include? :WFR
    end
    # Add write vectors to merge for read your writes.
    vector_merger.version_matrix <= (session_guarantees * write_vectors).pairs(
      :reqid => :reqid) do |guarantees, vector|
      vector if guarantees.session_types.include? :MW
    end
  end

  bloom :output_read_results do
    output_read_result <= (vector_aggregator.minimal_matrix * pending_reads).pairs(
      :reqid => :reqid, :v_vector => :v_vector) do |min,reads|
      [min.reqid, min.v_vector, reads.value]
    end
    output_read_result <= (vector_aggregator.minimal_matrix * read_vector).pairs(
      :reqid => :reqid) do |min,read_vec|
      [min.reqid, read_vec.v_vector, nil]
    end
  end

  bloom :output_write_results do
    output_write_result <= vector_merger.merge_vector
  end

  # Remove requests that have been cleared.
  bloom :cleanup_results do
    read_vectors <+ read_vectors.notin(end_requests, :reqid => :reqid)
    write_vectors <+ write_vectors.notin(end_requests, :reqid => :reqid)
    session_guarantees <+ session_guarantees.notin(end_requests, :reqid => :reqid)
    pending_reads <+ pending_reads.notin(end_requests, :reqid => :reqid)
    pending_writes <+ pending_writes.notin(end_requests, :reqid => :reqid)
  end

end
