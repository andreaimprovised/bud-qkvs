require 'rubygems'
require 'bud'
require 'backports'

# =============
# Vector Clocks
# =============
# This library handles the logic of updating and comparing vector
# clocks. A vector clock is a vector of logical clocks, namely Lamport
# Timestamps, which model agents in a distributed system.  Each agent
# has a unique id, which refer to with the field :a_ident.  Agents can
# participate in different groups in a distributed system; these are
# referred to by the field :g_ident.  The local system can have a single
# agent per group be considered the "local" agent.
#
# Local Updating
# --------------
# Every timestep of the Bud evaluator, the logical clock of the local
# agent is incremented.  This models the behavior of the evaluator, in that
# the program's state can only change every timestep.
#
# Remote Updating
# ---------------
# When messages are passed between nodes, a vector clock can be
# attached to that message, then it is referred to as a remote signal.
# Upon reception, the local node will compute a new vector clock given
# the information from the one received.  The sender of the message
# must be specified by the field :s_ident.  This sender must be an
# agent part of the vector clock and be equivalent to one :a_ident.
#
# Multiple Updates
# ----------------
# If several vector clocks are received in a particular timestep, or you
# would like to process several received vector clocks in the same timestep,
# you can.  Each remote signal has an identifier which is unique to a 
# particular sender, specified by :m_ident.
#
# Output Behavior:
# ----------------
# This module will render an event clock every timestep. If no input
# is specified in a timestep, the clock will reflect only a local update for
# each local agent.  If inputs are specified, the clock will immediately
# reflect the consequence of observing those new clocks in the same timestep.
#
# Vector Clock Storage:
# -------------------
# This module does not store old versions of the clock which
# correspond to a previous event.  It is up to external users to read
# the resulting vector clock associated with the event and place it in
# storage if needed.
#
# Dynamic Addition of Agents:
# -----------------------------
# This module supports dynamic addition (not subtraction!) of new
# processes in the system and groups as well.  If a (group,agent) pair
# is not present in a vector clock, it is assumed to be 0 at the local
# agent.
module VectorClockProtocol
  state do
    interface input, :set_local_agent, [:g_ident] => [:a_ident]
    interface input, :remote_signal, [:m_ident, :s_ident, :a_ident] => [:stamp]
    interface input, :remote_sender, [:m_ident, :s_ident] => [:g_ident]
    interface output, :event_clock, [:g_ident, :a_ident] => [:stamp]
  end
end

# =====================
# Vector Clock Ordering
# =====================
#
# Computes a same timestep result of the comparison between 
# events x and y:
#
# if VC(x)  --> VC(y) => result = -1   => x occured before y
# if VC(x) <--> VC(y) => result =  0   => x occured concurrently with y
# if VC(x) <--  VC(y) => result =  1   => x occured after y
module VectorClockOrderingProtocol
  state do
    interface input, :vc_x, [:g_ident, :a_ident] => [:stamp]
    interface input, :vc_y, [:g_ident, :a_ident] => [:stamp]
    interface output, :order, [:g_ident] => [:result]
  end
end

# =======================
# Vector Clock Serializer
# =======================
#
# Users of this module request to serialize or deserialize vector
# clocks.  Each request must have a unique identifier, :r_ident.
#
# The results of serialization and deserialization are synchronously
# computed in the same timestep.  These results are forgotten the next
# timestep!
module VectorClockSerializerProtocol
  state do
    # Serialization
    interface input, :serialize, [:r_ident, :g_ident, :a_ident] => [:stamp]
    interface output, :serialize_rs, [:r_ident] => [:vector]
    # Deserialization
    interface input, :deserialize, [:r_ident] => [:vector]
    interface output, :deserialize_rs, [:r_ident, :g_ident, :a_ident] => [:stamp]
  end
end

module VectorClock
  include VectorClockProtocol
  
  state do
    # Stores the counts for each clock
    scratch :v_clock, event_clock.schema

    # Records the name of the local agent
    table :local_agent, set_local_agent.schema

    # New vector clock
    scratch :new_v_clock, event_clock.schema

    # Matrix plus increments
    scratch :batch_v_clock, [:m_ident, :s_ident, :g_ident, :a_ident] => [:stamp]
  end

  # Set local agent, interface bootstrapping if you will
  bloom do
    local_agent <= set_local_agent
    new_v_clock <= set_local_agent { |x| [x.g_ident, x.a_ident, 0] }
  end

  bloom do

    batch_v_clock <= (v_clock * local_agent)\
      .pairs(:g_ident => :g_ident) do |x,y|
      if y.a_ident == x.a_ident
        [nil, y.a_ident, x.g_ident, x.a_ident, x.stamp + 1]
      else
        [nil, y.a_ident, x.g_ident, x.a_ident, x.stamp]
      end
    end

    batch_v_clock <= (remote_signal * remote_sender)\
      .pairs(:m_ident => :m_ident, :s_ident => :s_ident) do |x,y|
      if x.s_ident == x.a_ident
        [x.m_ident, x.s_ident, y.g_ident, x.a_ident, x.stamp + 1]
      else
        [x.m_ident, x.s_ident, y.g_ident, x.a_ident, x.stamp]
      end
    end

    new_v_clock <= batch_v_clock.group([:g_ident, :a_ident], max(:stamp))

    # Update backing sequence storage
    v_clock <+ new_v_clock

    # Update the event_clock output interface
    event_clock <= new_v_clock

  end
end


module VectorClockOrdering
  include VectorClockOrderingProtocol

  state do
    scratch :less, [:g_ident] => []
    scratch :more, [:g_ident] => []
  end

  bloom do
    # would be much nicer with a full outer join

    # compute all agents in x with clock lower than y
    less <= (vc_y * vc_x)\
      .outer(:a_ident => :a_ident, :g_ident => :g_ident) do |y,x|
      [y.g_ident] if Integer(x.stamp) < y.stamp
    end
    less <= (vc_x * vc_y)\
      .outer(:a_ident => :a_ident, :g_ident => :g_ident) do |x,y|
      [x.g_ident] if x.stamp < Integer(y.stamp)
    end

    # compute all agents in x with clock greater than y
    more <= (vc_y * vc_x)\
      .outer(:a_ident => :a_ident, :g_ident => :g_ident) do |y,x|
      [y.g_ident] if Integer(x.stamp) > y.stamp
    end
    more <= (vc_x * vc_y)\
      .outer(:a_ident => :a_ident, :g_ident => :g_ident) do |x,y|
      [x.g_ident] if x.stamp > Integer(y.stamp)
    end

    # if more is empty and less has at least 1 thing, then -1
    # if less is empty and more has at least 1 thing, then 1
    # if more and less have stuff, then 0

    temp :more_order <= more.notin(less, :g_ident => :g_ident)
    temp :less_order <= less.notin(more, :g_ident => :g_ident)
    temp :both <= (more * less).lefts(:g_ident => :g_ident)

    order <= more_order { |m| [m.g_ident, 1] }
    order <= less_order { |l| [l.g_ident, -1] }
    order <= both { |b| [b.g_ident, 0] }
  end
end

module VectorClockSerializer
  include VectorClockSerializerProtocol

  bloom do
    serialize_rs <= serialize.reduce({}) do |meta, x|
      meta[x.r_ident] ||= []
      meta[x.r_ident] << [x.g_ident, x.a_ident, x.stamp]
      meta
    end

    deserialize_rs <= deserialize.flat_map do |x|
      x.vector.map do |y|
        [x.r_ident, y[0], y[1], y[2]]
      end
    end
  end

end
