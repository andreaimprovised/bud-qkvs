require 'rubygems'
require 'bud'
require 'sequences'

# This module supports the gossiping of messages and members, specifically each node will randomly choose
# a subset of its known members and resend out the messages and members they have not seen yet.
#
# Any input into add_member and/or remove_member will update the known members of the entire set of gossiping nodes.
# Over time, more nodes will learn about these newly added/removed members.
# When a node receives a message from a sender it does not know about, the node will add the sender to its list of known members.
#
# Any input into send_message will be propagated to the known members.
# Over time, more nodes will receive the message.
#
# Once a node receives a message, the message will be outputted to recv_message.
#
module GossipProtocol
  import Counter => :c

  state do
    # Any messages inputted into send_message will be propagated out to known members
    interface input, :send_message, [:message, :msgid]
    # Once a node receives a message, messages will be outputted to recv_message
    interface output, :recv_message, [:message]
    # Any member messages inputted into add_member will be propagated out to known members.
    # Over time, more nodes will learn more about the newly added members.
    interface input, :add_member, [:member, :msgid]
    # Any member messages inputted into remove_member will be propagated out to known members.
    # Over time, more nodes will learn more about the newly removed members.
    interface input, :remove_member, [:member, :msgid]

    channel :message_chan, [:@dst, :from, :msg_type, :msgid] => [:message]

    table :messages, [:host, :msgid]
    scratch :messages_to_output, message_chan.schema
    table :messages_to_send, message_chan.schema
    scratch :sent_messages, message_chan.schema

    table :members, [:host]
    scratch :chosen_member, [:host]
    scratch :members_to_send, [:host]
    scratch :unchosen_members, [:host]
    scratch :message_msgid, [:msgid]
    scratch :new_members, message_chan.schema
  end

  bloom :chosen_message do
    # assign a probability to each member to determine if they are chosen to be sent a message
    chosen_member <= members.group([], choose_rand(:host))
    unchosen_members <= members.notin(chosen_member, :host => :host)
    members_to_send <= unchosen_members { |x| x if (rand(10) < 2) }
    members_to_send <= chosen_member { |x| [x.host] }
    seq.increment <= [["counter"]]
    seq.get_count <= [["counter"]]
    chosen_message <= messages_to_send.group([], chooserand)
    # send all the messages to the chosen members
    sent_messages <= (chosen_message * seq.return_count * members_to_send).combos { |l, m, r|
      if r.host != ip_port
        [r.host, l.from, l.msg_type, m.tally, l.message]
      end
    }
    message_chan <~ sent_messages
    messages_to_send <- sent_messages { |x| ["", x.from, x.msg_type, x.msgid, x.message] }
    messages <+- sent_messages { |x| [x.from, x.msgid] }
  end

  bloom :receive_message do
    # store all messages in the channel that are not in messages table
    messages_to_output <= message_chan.notin(messages, :from => :host, :msgid => :msgid)
    messages <+ messages_to_output { |x| [x.from,x.msgid] }
    messages_to_send <+ messages_to_output { |x| ["", x.from, x.msg_type, x.msgid, x.message] }

    # determine what type of message was received
    recv_message <+ messages_to_output { |x| [x.message] if x.msg_type == "message" }
    members <+- messages_to_output { |x| [x.message] if x.msg_type == "add_member" }
    new_members <= message_chan.notin(members, :from => :host)
    members <+- new_members { |x|
      [x.from] if x.msg_type != "remove_member"
    }
    members <- messages_to_output { |x| [x.message] if x.msg_type == "remove_member" }
  end

  bloom :send_message do
    # we put "" because we do not care about the sender
    messages_to_send <+ send_message { |x|
      ["", ip_port, "message", x.msgid, x.message]
    }
  end

  bloom :membership_sending do
    members <+ add_member { |x| [x.member] }
    members <- remove_member { |x| [x.member] }
    messages_to_send <+ add_member { |x|
      ["", ip_port, "add_member", x.msgid, x.member]
    }
    messages_to_send <+ remove_member { |x|
      ["", ip_port, "remove_member", x.msgid, x.member]
    }
  end

end
