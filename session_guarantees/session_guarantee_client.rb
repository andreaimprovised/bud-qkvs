module SessionQuorumKVSClientProtocol
  interface input, :create_session, [:reqid] => [:session_types]
  interface input, :kvget, [:session_id, :reqid] => [:key]
  interface input, :kvput, [:session_id, :reqid] => [:key, :value]
  interface input, :kvdel, [:session_id, :reqid] => [:key]

  interface input, :session_response, [:reqid] => [:session_id]
  interface output, :kvget_response, [:reqid, :value]
  interface output, :kvputdel_response, [:reqid]
end


module SessionQuorumKVSClient
  require SessionQuorumKVSClientProtocol
  require SessionQuorumKVSNodeProtocol

  bootstrap do
  end

  state do
    table :sessions, [:session_id] => [:session_types]
    table :read_vectors, [:session_id] => [:read_vector]
    table :write_vectors, [:session_id] => [:write_vector]
  end

  bloom :init_sessions do
    # Uses budtime as the session_id
    sessions <= create_session.argagg(:choose_rand, [], :reqid) { |a| [@budtime, a[1]] }
    sessions_response <= create_session.argagg(:choose_rand, [], :reqid) { |a| [a[0], @budtime] }
    # How do you initialize read/write_vector...
  end

  bloom :request_read do
    kvread <= (kvget * sessions * read_vectors).pairs(kvget.session_id => sessions.session_id,
                                                      sessions.session_id => read_vectors.session_id) do |r, s, v|
      [r.reqid, r.key, s.session_types, v.read_vector]
    end
  end

  bloom :request_write do
    kvwrite <= (kvdel * sessions * write_vectors).pairs(kvget.session_id => sessions.session_id,
                                                        sessions.session_id => write_vectors.session_id) do |r, s, v|
      [r.reqid, r.key, nil, s.session_types, v.write_vector]
    end

    kvwrite <= (kvput * sessions * write_vectors).pairs(kvget.session_id => sessions.session_id,
                                                        sessions.session_id => write_vectors.session_id) do |r, s, v|
      [r.reqid, r.key, r.value, s.session_types, v.write_vector]
    end
  end

  bloom :respond_to_write do
    kvputdel_response <= kvwrite_response{|r| [r.reqid]}
  end

  bloom :respond_to_read do
    kvputdel_response <= kvwrite_response{|r| [r.reqid, r.value]}
  end

end

module SessionQuorumKVSProtocol
  interface input, :quorum_config, [] => [:r_fraction, :w_fraction] # ?
  interface input, :kvread, [:reqid] => [:key, :session_types, :read_vector]
  interface input, :kvwrite, [:reqid] => [:key, :value, :session_types, :write_vector]

  interface output, :kvread_response, [:reqid, :read_vector] => [:value]
  interface output, :kvwrite_response, [:reqid] => [:write_vector]
end

# TODO copy most of code from quorum kvs hw3 and make it compatible
module SessionQuorumKVS

  state do
  end

end

