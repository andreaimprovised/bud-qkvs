#!/usr/bin/ruby
require 'rubygems'
require 'bud'
require 'membership/membership'
require 'kvs/kvs'

module QuorumKVSProtocol
  state do
    interface input, :quorum_config, [] => [:r_fraction, :w_fraction]
    interface input, :kvput, [:client, :key] => [:reqid, :value]
    interface input, :kvdel, [:key] => [:reqid]
    interface input, :kvget, [:reqid] => [:key]
    interface output, :kvget_response, [:reqid] => [:key, :value]
    interface output, :kv_acks, [:reqid]
  end

end


module VoteCounter
 
  state do
    periodic :clock, 0.1
    #votes_required is the number of votes required per 
    #reqid before counting begins.
    table :raw_votes, [:client, :reqid, :key, :value, :version, :rtype]
    table :completed_votes, [:reqid]
    table :w_required, [] => [:num]
    table :r_required, [] => [:num]
    scratch :out, [:reqid] => [:key, :value]
    interface input, :votes_input, raw_votes.schema
    interface input, :votes_threshold, [:rtype] => [:num]
    interface output, :votes_output, raw_votes.schema
    table :votes_required, [:rtype] => [:num]
    scratch :winning_votes, raw_votes.schema
    scratch :complete_quorums, raw_votes.schema
    scratch :non_nil_quorums, raw_votes.schema
  end
  
  bloom :setup_counts do
    r_required <= [votes_threshold {|v| v.num if v.rtype == :g}]
    w_required <= [votes_threshold {|v| v.num if v.rtype == :p}]
  end

  bloom :process_votes do
    complete_quorums <= (raw_votes * raw_votes.group([:reqid], count)).pairs do |m,k|
      m if k[0] == m.reqid and
        ((m.rtype == "g" and k[1] >= r_required.first.num) or 
         (m.rtype == "p" and k[1] >= w_required.first.num))
    end
    completed_votes <+ complete_quorums {|q| [q.reqid]}
    winning_votes <= (complete_quorums * complete_quorums.group([:reqid], max(:version))).pairs do |m,k|
      m if m.reqid==k[0] and m.version == k[1]
    end
    raw_votes <- winning_votes
    votes_output <= winning_votes
  end

  bloom :glue do
    raw_votes <= votes_input.notin(completed_votes, :reqid => :reqid)
    votes_required <= votes_threshold
    #votes_output <= out
  end

end

# Same as BasicKVS but returns nil for get response if the key doesn't exist
module NilBasicKVS
  include KVSProtocol

  state do
    table :kvstate, [:key] => [:value]
    scratch :non_existing_keys, [:reqid] => [:key]
  end

  bloom :mutate do
    kvstate <+ kvput {|s|  [s.key, s.value]}
    kvstate <- (kvstate * kvput).lefts(:key => :key)
  end

  bloom :debug do
    #stdio <~ kvget_response.inspected
  end

  bloom :get do
    temp :getj <= (kvget * kvstate).pairs(:key => :key)
    non_existing_keys <= kvget do |m|
      m if not kvstate.exists?{|k| m.key == k.key}
    end
    kvget_response <= getj do |g, t|
      [g.reqid, t.key, t.value]
    end
    kvget_response <= non_existing_keys do |g| # added
      [g.reqid, g.key, nil]
    end
  end

  bloom :delete do
    kvstate <- (kvstate * kvdel).lefts(:key => :key)
  end
end



module LocalKVSHandler
  import NilBasicKVS => :kvs

  state do
    interface input, :kvput, [:reqid] => [:key, :value, :version]
    interface input, :kvdel, [:reqid] => [:key, :version]
    interface input, :kvget, [:reqid] => [:key, :version]
    interface output, :kv_response, [:reqid] => [:key, :value, :version, :rtype]
  end
  
  bloom :send_to_kvs do
    kvs.kvput <= kvput {|m| [0, m.key, m.reqid, m.value] }
    kvs.kvdel <= kvdel {|m| [m.key, m.reqid] }
    kvs.kvget <= kvget {|m| [m.reqid, m.key] }
  end
  
  bloom :debug do
    #stdio <~ kvs.kvget_response.inspected
    #stdio <~ kvget.inspected
  end

  bloom :receive_from_kvs do
    kv_response <+ kvs.kvget_response {|r| [r.reqid, r.key, r.value, 0, :g]}
    #kvdel and kvput will declare victory immediately.
    kv_response <+ kvdel {|r| [r.reqid, r.key, nil, 0, :d]}
    kv_response <+ kvput {|r| [r.reqid, r.key, r.value, 0, :p]}
  end
  
end

module QuorumKVS
  include QuorumKVSProtocol
  #  included for reference
  #  interface input, :quorum_config, [] => [:r_fraction, :w_fraction]
  #  interface input, :kvput, [:client, :key] => [:reqid, :value]
  #  interface input, :kvdel, [:key] => [:reqid]
  #  interface input, :kvget, [:reqid] => [:key]
  #  interface output, :kvget_response, [:reqid] => [:key, :value]
  #  interface output, :kv_acks, [:reqid]
  include StaticMembership
  import LocalKVSHandler => :kvs_handler
  import VoteCounter => :vote

  state do
    scratch :requests, [:reqid] => [:key, :value, :version, :rtype]
    # Collections for communicating with other nodes.
    channel :kvput_chan, [:@dest, :from, :reqid] => [:key, :value, :version]
    channel :kvdel_chan, [:@dest, :from, :reqid] => [:key, :version]
    channel :kvget_chan, [:@dest, :from, :reqid] => [:key, :version]
    table :unacked, [:reqid, :from, :mylocation]
    # Channels for specifying responses from other nodes.
    channel :kv_response, [:@dest, :from, :reqid] => [:key, :value, :version, :rtype]
    # Table that stores requests before they are completed.
    table :kv_requests, [:reqid] => [:key, :value, :version, :rtype]
    # Tables for counting and choosing nodes.
    table :n_required, [:rtype] => [:n_nodes]
    table :nodes, [:host] => [:ident]
    scratch :n_nodes, [] => [:n_nodes]
    scratch :acked_requests, requests.schema
  end

  bloom :node_setup do
    n_nodes <= member.group([], count)
    n_required <= (quorum_config * n_nodes).pairs {|c, n| [:g, c.r_fraction * n.n_nodes]}
    n_required <= (quorum_config * n_nodes).pairs {|c, n| [:p, c.r_fraction * n.n_nodes]}
    n_required <= (quorum_config * n_nodes).pairs {|c, n| [:d, c.r_fraction * n.n_nodes]}
    vote.votes_threshold <= n_required
    nodes <= member.notin(local_id, :ident => :ident)
  end
 
  bloom :init_requests do
    requests <= kvput {|p| [p.reqid, p.key, p.value, 0, :p]}
    requests <= kvget {|g| [g.reqid, g.key, nil, 0, :g]}
    requests <= kvdel {|d| [d.reqid, d.key, nil, 0, :d]}
    kv_requests <= requests
  end

  bloom :send_requests do
    kvput_chan <~ (nodes*requests).pairs do |c, r|
      [c.host, ip_port, r.reqid, r.key, r.value, r.version] if r.rtype == :p
    end
    kvdel_chan <~ (nodes*requests).pairs do |c, r|
      [c.host, ip_port, r.reqid, r.key, r.value, r.version] if r.rtype == :d
    end
    kvget_chan <~ (nodes*requests).pairs do |c, r|
      [c.host, ip_port, r.reqid, r.key, r.value, r.version] if r.rtype == :g
    end
  end

  bloom :receive_responses do
    vote.votes_input <+ kv_response {|m| [m.from, m.reqid, m.key, m.value, m.version, m.rtype]}
  end

  bloom :debug2 do
    temp :log_kv_response <= (unacked * kvs_handler.kv_response).pairs(:reqid => :reqid) do |u,k| 
      [u.mylocation, u.from ,k.reqid, k.key, k.value, k.version, k.rtype]
    end
    temp :log_kvget_chan <= kvget_chan {|m| [m.dest, m.from, m.reqid, m.key, m.version]}
  end

  bloom :respond_to_requests do
    unacked <= kvput_chan{|m| [m.reqid, m.from, m.dest]}
    unacked <= kvdel_chan{|m| [m.reqid, m.from, m.dest]}
    unacked <= kvget_chan{|m| [m.reqid, m.from, m.dest]}
    unacked <- (unacked * kvs_handler.kv_response).lefts(:reqid => :reqid)
    kvs_handler.kvput <+ kvput_chan {|m| [m.reqid, m.key, m.value, m.version]}
    kvs_handler.kvdel <+ kvdel_chan {|m| [m.reqid, m.key, m.version]}
    kvs_handler.kvget <+ kvget_chan {|m| [m.reqid, m.key, m.version]}
    kv_response <~ (unacked * kvs_handler.kv_response).pairs(:reqid => :reqid) do |u,k| 
      [u.from, u.mylocation ,k.reqid, k.key, k.value, k.version, k.rtype]
    end
  end

  bloom :end_requests do
    acked_requests <= (vote.votes_output * kv_requests).pairs(:reqid => :reqid) do |m,k|
      [m.reqid, m.key, m.value, m.version, m.rtype]
    end
    kvget_response <+ acked_requests {|r| [r.reqid, r.key, r.value] if r.rtype == "g"}
    kv_acks <+ acked_requests {|r| [r.reqid]}
    kv_requests <+ kv_requests.notin(acked_requests, :reqid => :reqid)
  end
end
