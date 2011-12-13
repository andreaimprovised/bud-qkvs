require 'rubygems'
require 'bud'

# This module will be used on a QuorumKVS as a voting module to pass
# results to. Results that satisfy the required guarantees are output
# as satisfiers.
module SessionVoteCounterProtocol
  # TODO this should be per-request.
  interface input, :set_guarantees, [:session_type] => [] # One(?) of :RYW, :MR, :WFR, :MW
  interface input, :vote_read, [:client, :reqid] => [:value, :v_vector]
  interface input, :vote_write, [:client, :reqid] => [:v_vector]
  interface output, :vote_read_satisfier, [:reqid] => [:value, :v_vector]
  interface output, :vote_write_satisfier, [:reqid] => [:v_vector]

  # TODO add some interface to initialize a vote
  # (add reqid => {read,write}vector.)
end

module SessionVoteCounter
  include SessionVoteCounterProtocol
  require VersionVectonDominator => :compare_vectors
  require VersionVectonConcurrency => :merge_vectors

  state do
    # TODO consider having this as two deserialized vectors
    # TODO vectors are now per_request. Update code below.
    table :read_vector, [:reqid] => [:vector]
    table :write_vector, [:reqid] => [:vector]
    # List of guarantees. For version 1, assume it's only one, and then
    # we'll see if it's easy to guarantee them all at the same time.
    table :session_guarantees, [:type]
    table :potential_read_satisfiers, vote_read.schema
    table :potential_write_satisfiers, vote_write.schema
    # Results that satisfied a guarantee.
    scratch :satisfiers, [:client, :reqid]
    # Results that did not satisfy a guarantee.
    scratch :non_satisfiers, [:client, :reqid]
  end

  bloom :init_session do
    session_guarantees <= set_guarantees
  end

  # Store input => potential satisfiers
  bloom :hold_vote do
    potential_read_satisfiers <= vote_read
    potential_write_satisfiers <= vote_write
  end

  # For compare_*, put the right vectors into compare_vectors.
  # Request could be a unique (:client + :reqid) pair
  # TODO consider a better serialization scheme (list, similar to what andy did?)

  bloom :compare_mr_read do
    compare_vectors.domination_query <= (session_guarantees * vote_read * read_vector).combos do |sg, vread, rvec|
      ["#{vread.client}-#{vread.reqid}", rvec.vector, vread.v_vector] if sg.type == :MR
    end
  # if MR then
  #    check S.vector dominates read-vector
  end

  bloom :compare_ryw_read do
    compare_vectors.domination_query <= (session_guarantees * vote_read * write_vector).combos do |sg, vread, wvec|
      ["#{vread.client}-#{vread.reqid}", wvec.vector, vread.v_vector] if sg.type == :RYW
    end
  # if RYW then
  #       check S.vector dominates write-vector
  end

  bloom :compare_wfr_write do
    compare_vectors.domination_query <= (session_guarantees * vote_write * read_vector).combos do |sg, vwrite, rvec|
      ["#{vwrite.client}-#{vwrite.reqid}", rvec.vector, vwrite.v_vector] if sg.type == :WFR
    end
  # if WFR then
  #      check S.vector dominates read-vector
  end

  bloom :compare_mw_write do
    compare_vectors.domination_query <= (session_guarantees * vote_write * write_vector).combos do |sg, vwrite, wvec|
      ["#{vwrite.client}-#{vwrite.reqid}", wvec.vector, vwrite.v_vector] if sg.type == :MW
    end
  # if MW then
  #      check S.vector dominates write-vector
  end

  # Take output from compare_vector and store the :client, reqid of
  # satisfiers and non_satisfiers in satisfiers or non_satisfiers scratch. Might need to do
  # :request => [:client, :reqid] extraction.
  bloom :store_compare_results do
    satisfiers <= compare_vectors.result do |result|
      result.request.split("-") if order == 1
    end
    non_satisfiers <= compare_vectors.result do |result|
      result.request.split("-") if order != 1
    end
  end

  # TODO should check_* be split into multiple rules?

  # Join satisfiers on potential read satisfiers, update read-vector, and
  # return the correct max.
  bloom :check_read_satisfiers do
  #TODO finish
    # Add relevant-write-vector from read result to MAX.
    merge_vectors.version_matrix <= (potential_read_satisfiers * satisfiers).pairs(
      :reqid => :reqid, :client => :client) do |pw, w|
      ["#{w.reqid}-#{w.client}", pw.v_vector]
    end
    # Add read-vector from passed-in query.
    merge_vectors.version_matrix <= (read_vector * satisfiers).pairs(
      :reqid => :reqid) do |rv, w|
      ["#{w.reqid}-#{w.client}", rv.vector]
    end
  # [result, relevant-write-vector] := read R from S
  #  read-vector := MAX(read-vector,
  #      relevant-write-vector)
  #  return result
  end

  bloom :output_read_satisfiers do
  end

  # Join satisfiers on potential write satisfiers, update write-vector, and
  # return the correct updated vector.
  # Something needs to be done about serialization or weird vector value setting...
  bloom :check_satisfier_write do
  # TODO finish
  # wid := write W to S
  #   write-vector[S] := wid.clock
  end

  # Remove satisfiers and non_satisfiers from potential_*_satisfiers.
  bloom :cleanup_results do
    potential_read_satisfiers <- satisfiers
    potential_read_satisfiers <- non_satisfiers
    potential_write_satisfiers <- satisfiers
    potential_write_satisfiers <- non_satisfiers
  end

end
