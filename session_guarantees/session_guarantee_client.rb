module SessionQuorumKVSClientProtocol
  interface input, :create_session, [:reqid] => [:session_types]
  interface input, :kvget, [:session_id, :reqid] => [:key]
  interface input, :kvput, [:session_id, :reqid] => [:key, :value]
  interface input, :kvdel, [:session_id, :reqid] => [:key]

  interface input, :session_response, [:reqid] => [:session_id]
  interface output, :kvget_response, [:session_id, :reqid] => [:value]
  interface output, :kvput_response, [:session, :reqid] => []
  interface output, :kvdel_response, [:session, :reqid] => []
end


module SessionQuorumKVSClient
  require SessionQuorumKVSClientProtocol
  require SessionQuorumKVSNodeProtocol

  state do
    table :sessions, [:session_id] => [:session_types]
    table :read_vectors, [:session_id] => [:read_vector]
    table :write_vectors, [:session_id] => [:write_vector]
    table :session_counter, [] => [:session_id]
  end
  
  bloom :bootstrap do
    :session_counter <= [[0]]
  end
  
  bloom :init_sessions do
  end

  bloom :request_read do
  end

  bloom :request_write do
  end

  bloom :request_delete do
  end

  bloom :respond_to_get do
  end

  bloom :respond_to_write do
  end

  bloom :respond_to_del do
  end

end

module SessionQuorumKVSProtocol
  interface input, :quorum_config, [] => [:r_fraction, :w_fraction] # ?
  interface input, :kvread, [:reqid] => [:key, :session_types, :read_vector]    
  interface input, :kvwrite, [:reqid] => [:key, :value, :session_types, :write_vector]

  interface output, :kvread_response, [:reqid] => [:value, :read_vector]
  interface output, :kvwrite_response, [:reqid] => [:write_vector]
end

# TODO copy most of code from quorum kvs hw3 and make it compatible
module SessionQuorumKVS

  state do
  end

end

