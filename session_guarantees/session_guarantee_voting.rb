require 'rubygems'
require 'bud'
require 'backports'

require "causality/version_vector"

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
    interface input, :add_read, [:v_vector, :reqid] => [:value]

    # Adds a 'vote' of a write result from a Quorum node.
    # @param [VersionVector] v_vector the version vector for the written
    #     value
    # @param [Object] reqid a unique id for the request
    interface input, :add_write, [:v_vector, :reqid]

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

    # NOTE if consistently outputing is a performance concern, make it
    # only output on end_request.
  end
end

# This module provides an implementation of the SessionVoteCounterProtocol.
module SessionVoteCounter
  include SessionVoteCounterProtocol
  import VersionVectorMerge => :vector_merger
  import VersionVectorConcurrency => :vector_aggregator
  import VersionMatrixSerializer => :matrix_serializer

  state do
    # Per-reqid set of vectors indicating the read set of the request.
    table :read_vectors, [:reqid, :v_vector]
    # Per-reqid vector indicating the write set of the request.
    table :write_vector, [:reqid] => [:v_vector]
    # Per-reqid list of requested session guarantees.
    table :session_guarantees, [:reqid] => [:session_types]
    # Table of all read results passed in.
    table :pending_reads, add_read.schema
    # Table of all write results passed in.
    table :pending_writes, add_write.schema
    # Temporary collection of non-read_vector read results.
    scratch :pre_read_results, output_read_result.schema
    # Temporary collection of requests with existing read results.
    scratch :non_empty_reads, [:reqid]
    # Temporary collection of aggregated read vectors.
    scratch :max_vectors, vector_aggregator.minimal_matrix.schema
  end

  bloom :init_session do
    # Init session guarantees.
    session_guarantees <= init_request {|init| [init.reqid, init.session_types]}
    # De-serialize and init read vectors.
    matrix_serializer.deserialize <= init_request do |init|
      [init.reqid, init.read_vectors]
    end
    read_vectors <= matrix_serializer.deserialize_ack
    # Set write vector.
    write_vector <= init_request {|init| [init.reqid, init.write_vector] }
  end

  bloom :aggregate_read_votes do
    pending_reads <= add_read
    # vector_aggregator will output the max set of read version vectors.
    vector_aggregator.version_matrix <= pending_reads {|r| [r.reqid, r.v_vector]}
  end

  bloom :handle_read_session_guarantees do
    # Add read vectors to aggregate for monotonic reads.
    vector_aggregator.version_matrix <= (session_guarantees * \
          read_vectors).matches do |guarantees, vector|
      vector if guarantees.session_types.include? :MR
    end
    # Add write vectors to aggregate for read your writes.
    vector_aggregator.version_matrix <= (session_guarantees * \
          write_vector).matches do |guarantees, vector|
      vector if guarantees.session_types.include? :RYW
    end
  end

  bloom :merge_write_votes do
    pending_writes <= add_write
    # vector_merger will merge all write vectors into a total max vector.
    vector_merger.version_matrix <= add_write {|w| [w.reqid, w.v_vector]}
  end

  bloom :handle_write_session_guarantees do
    # Add read vectors to merge for writes follow reads.
    vector_merger.version_matrix <= (session_guarantees * \
          read_vectors).matches do |guarantees, vector|
      vector if guarantees.session_types.include? :WFR
    end
    # Add write vectors to merge for read your writes.
    vector_merger.version_matrix <= (session_guarantees * \
          write_vector).matches do |guarantees, vector|
      vector if guarantees.session_types.include? :MW
    end
  end

  bloom :output_read_results do
    max_vectors <= vector_aggregator.minimal_matrix
    # Get all non-read_vector results.
    pre_read_results <= (max_vectors * pending_reads).pairs( \
          :request => :reqid, :v_vector => :v_vector) do |max,reads|
      [max.request, max.v_vector, reads.value]
    end
    non_empty_reads <= pre_read_results {|result| result.reqid }
    # Add the read_vector result if the results exist.
    output_read_result <= (max_vectors * read_vectors).pairs( \
          :request => :reqid) do |max,read_vec|
      [max.request, read_vec.v_vector, nil] if non_empty_reads.include?([max.reqid])
    end
  end

  bloom :output_write_results do
    # Output the new merge write vector.
    output_write_result <= vector_merger.merge_vector
  end

  # Remove requests that have been cleared.
  bloom :cleanup_results do
    read_vectors <+ read_vectors.notin(end_request, :reqid => :reqid)
    write_vector <+ write_vector.notin(end_request, :reqid => :reqid)
    session_guarantees <+ session_guarantees.notin(end_request, :reqid => :reqid)
    pending_reads <+ pending_reads.notin(end_request, :reqid => :reqid)
    pending_writes <+ pending_writes.notin(end_request, :reqid => :reqid)
  end

end
