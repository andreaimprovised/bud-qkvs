# Mix in with client. Abstracts away version vectors for the user.
module SessionQuorumKVSClientProtocol
  state do
    interface input, :create_session, [:reqid] => [:session_types]
    interface input, :kvget, [:session_id, :reqid] => [:key]
    interface input, :kvput, [:session_id, :reqid] => [:key, :value]
    interface input, :kvdel, [:session_id, :reqid] => [:key]

    interface input, :session_response, [:reqid] => [:session_id]
    interface output, :kvget_response, [:reqid, :value]
    interface output, :kvputdel_response, [:reqid]
  end
end

# Mix in with client and node. Communication interface between client and node.
module SessionQuorumKVSProtocol
  state do
    interface input, :quorum_config, [] => [:r_fraction, :w_fraction] # ?
    interface input, :kvread, [:reqid] => [:key, :session_types, :read_vector, \
          :write_vector]
    interface input, :kvwrite, [:reqid] => [:key, :value, :session_types, \
          :read_vector, :write_vector]
    interface output, :kvread_response, [:reqid, :read_vector] => [:value]
    interface output, :kvwrite_response, [:reqid] => [:write_vector]
  end
end

# Main client-side module. Maintains session information.
module SessionQuorumKVSClient
  include SessionQuorumKVSClientProtocol
  include SessionQuorumKVSProtocol

  state do
    table :sessions, [:session_id] => [:session_types]
    table :read_vectors, [:session_id] => [:read_vector]
    table :write_vectors, [:session_id] => [:write_vector]
    table :reqid_session_map, [:reqid] => [:session_id]
    scratch :chosen_create_session_req, [] => [:reqid, :session_types]
  end

  # Handles session creation requests. One request is completed per tick and budtime is
  # used as the session_id.
  bloom :init_sessions do
    chosen_create_session_req <= create_session.argagg(:choose_rand, [], :reqid)
    sessions <= chosen_create_session_req { |a| [@budtime, a[1]] }
    session_response <= chosen_create_session_req { |a| [a[0], @budtime] }
    read_vectors <= chosen_create_session_req {|a| [@budtime, []]}
    write_vectors <= chosen_create_session_req {|a| [@budtime, []]}
    reqid_session_map <= chosen_create_session_req {|a| [a[0], @budtime]}
  end

  # Attaches session information (read vectors) to a kvget request
  bloom :request_read do
    kvread <= (kvget * sessions * read_vectors).pairs(kvget.session_id => sessions.session_id,
                                                      sessions.session_id => read_vectors.session_id) do |r, s, v|
      [r.reqid, r.key, s.session_types, v.read_vector]
    end
  end

  # Attaches session information (write vectors) to a kvput or kvdel request. (kvdel is just a kvput with value = nil)
  bloom :request_write do
    kvwrite <= (kvdel * sessions * write_vectors).pairs(kvdel.session_id => sessions.session_id,
                                                        sessions.session_id => write_vectors.session_id) do |r, s, v|
      [r.reqid, r.key, nil, s.session_types, v.write_vector]
    end

    kvwrite <= (kvput * sessions * write_vectors).pairs(kvput.session_id => sessions.session_id,
                                                        sessions.session_id => write_vectors.session_id) do |r, s, v|
      [r.reqid, r.key, r.value, s.session_types, v.write_vector]
    end
  end

  # Updates session information (write vectors) and passes results back to client.
  bloom :respond_to_write do
    kvputdel_response <= kvwrite_response{|r| [r.reqid]}
    write_vectors <+- (reqid_session_map * kvwrite_response).pairs(:reqid => :reqid) do |s, w|
      [s.session_id, w.write_vector]
    end
  end

  # Updates session information (read vectors) and passes results back to client.
  bloom :respond_to_read do
    kvputdel_response <= kvwrite_response{|r| [r.reqid, r.value]}
  end

end
