require 'rubygems'
require 'test/unit'
require 'bud'
require 'gossip'

class GP
  include Bud
  include GossipProtocol

  state do
    table :output_messages, [:message]
  end

  bloom :store_messages do
    output_messages <= recv_message
  end
end


class TestGossip < Test::Unit::TestCase

  def machine_step(machines)
    (0...10).each { |x|
      puts "Machine #{x} debug"
#       puts machines[x].messages_to_send.inspected
#       puts machines[x].messages_to_output.inspected
#       puts machines[x].message_chan.inspected
      puts machines[x].messages.inspected
#      puts machines[x].recv_message.inspected
      puts ""
      machines[x].sync_do
    }
  end

  def test_gossip
    num_nodes = 40
    machines = Array.new(num_nodes)
    (0...num_nodes).each { |x|
      puts "Starting runtime #{x}"
      machines[x] = GP.new(:port => 12340 + x)
      machines[x].run_bg
    }

    (1...num_nodes).each { |x|
      port = 12340 + x
      puts "Adding runtime #{x} to cluster"
      machines[0].sync_do { machines[0].add_member <+ [["127.0.0.1:#{port}", 1000 + x]] }
    }

    puts "Adding members..."
    1.times { machine_step(machines) }

    puts "Gossiping..."
    (0...num_nodes).each { |x|
      message = "Hello from #{x}"
      machines[x].sync_do { machines[x].send_message <+ [[message, 10000 + x]] }
    }

    machine_step(machines)

    (0...num_nodes).each { |x|
      puts "Messages known to runtime  #{x}"
      puts machines[x].output_messages.inspected
    }

    (0...num_nodes).each { |x|
      puts "Nodes known to runtime  #{x}"
      puts machines[x].members.inspected
    }

    puts "Checking gossiping..."
    (0...num_nodes).each { |x|
      assert_equal(num_nodes, machines[x].output_messages.length)
    }
   end
end
