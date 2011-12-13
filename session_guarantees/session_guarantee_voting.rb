
# This module will be used on a QuorumKVS as a voting module to pass
# results to. Results that satisfy the required guarantees are output
# as winners.
module SessionVoteCounterProtocol
  interface input, :set_guarantees, [:session_type] => [] # One(?) of :RYW, :MR, :WFR, :MW
  interface input, :vote_read, [:client, :reqid] => [:value, :v_vector]
  interface input, :vote_write, [:client, :reqid] => [:v_vector]
  interface output, :vote_read_winner, [:reqid] => [:value, :v_vector]
  interface output, :vote_write_winner, [:reqid] => [:v_vector]

  # TODO add some interface to initialize a vote
  # (add reqid => {read,write}vector.)
end

module SessionVoteCounter
  include SessionVoteCounterProtocol
  require VersionVectonDominator => :compare_vectors

  state do
    # TODO consider having this as two deserialized vectors
    # TODO vectors are now per_request. Update code below.
    table :read_vector, [:reqid] => [:vector]
    table :write_vector, [:reqid] => [:vector]
    # List of guarantees. For version 1, assume it's only one, and then
    # we'll see if it's easy to guarantee them all at the same time.
    table :session_guarantees, [:type]
    table :potential_read_winners, vote_read.schema
    table :potential_write_winners, vote_write.schema
    # Results that satisfied a guarantee.
    scratch :winners, [:client, :reqid]
    # Results that did not satisfy a guarantee.
    scratch :losers, [:client, :reqid]
  end

  bloom :init_session do
    session_guarantees <= set_guarantees
  end

  # Store input => potential winners
  bloom :hold_vote do
    potential_read_winners <= vote_read
    potential_write_winners <= vote_write
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
  # winners and losers in winners or losers scratch. Might need to do
  # :request => [:client, :reqid] extraction.
  bloom :store_compare_results do
    winners <= compare_vectors.result do |result|
      result.request.split("-") if order == 1
    end
    losers <= compare_vectors.result do |result|
      result.request.split("-") if order != 1
    end
  end

  # TODO should check_* be split into multiple rules?

  # Join winners on potential read winners, update read-vector, and
  # return the correct max.
  bloom :check_winner_read do
  #TODO finish
    (potential_read_winners * winners).pairs(:reqid => :reqid, :client => :client) do |pw, w|
    end
  # [result, relevant-write-vector] := read R from S
  #  read-vector := MAX(read-vector,
  #      relevant-write-vector)
  #  return result
  end

  # Join winners on potential write winners, update write-vector, and
  # return the correct updated vector.
  # Something needs to be done about serialization or weird vector value setting...
  bloom :check_winner_write do
  # TODO finish
  # wid := write W to S
  #   write-vector[S] := wid.clock
  end

  # Remove winners and losers from potential_*_winners.
  bloom :cleanup_results do
    potential_read_winners <- winners
    potential_read_winners <- losers
    potential_write_winners <- winners
    potential_write_winners <- losers
  end

end
