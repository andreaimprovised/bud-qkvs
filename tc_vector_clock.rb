require 'rubygems'
require 'bud'
require 'causality/vector_clock'
require 'test/unit'
require 'print/pprint'

class TestVectorClocks < Test::Unit::TestCase

  class LocalVC
    include Bud
    include VectorClock
  end

  class RemoteVC
    include Bud
    include VectorClock
    include PPrint
  end

  class Ordering
    include Bud
    include VectorClockOrdering
  end

  class Serializer
    include Bud
    include VectorClockSerializer
  end

  def setup
    @vc1 = LocalVC.new
    @vc2 = RemoteVC.new
    @vc3 = Ordering.new
    @vc4 = Serializer.new
  end

  def teardown
    # nothin
  end

  def set_assert_equal(arg1, arg2)
    assert_equal(arg1.sort, arg2.sort)
  end

  def test_a_local
    p "test_a_local"
    @vc1.set_local_agent <+ [[0,0],[1,1]]
    @vc1.tick
    set_assert_equal([[0,0,0],[1,1,0]], @vc1.event_clock.to_a)

    # local update
    @vc1.tick
    set_assert_equal([[0, 0, 1],[1, 1,1]], @vc1.event_clock.to_a)

    # local update
    @vc1.tick
    set_assert_equal([[0, 0, 2],[1, 1, 2]], @vc1.event_clock.to_a)

    # local update
    @vc1.tick
    set_assert_equal([[0, 0, 3],[1, 1, 3]], @vc1.event_clock.to_a)
  end

  def test_b_remote
    p "Test b remote"
    @vc2.set_local_agent <+ [[0,0]]
    @vc2.tick

    @vc2.remote_signal <+ [[100,2,0,0], [100,2,1,2], [100,2,2,3]]
    @vc2.remote_sender <+ [[100,2,0]]
    @vc2.tick
    set_assert_equal([[0,0,1], [0,1,2], [0,2,4]], @vc2.event_clock.to_a)

    # local update
    @vc2.tick
    set_assert_equal([[0,0,2], [0,1,2], [0,2,4]], @vc2.event_clock.to_a)

    @vc2.remote_signal <+ [[101,1,1,6], [101,1,2,7]]
    @vc2.remote_sender <+ [[101,1,0]]
    @vc2.tick
    set_assert_equal([[0,0,3], [0,1,7], [0,2,7]], @vc2.event_clock.to_a)

    @vc2.remote_signal <+ [[102,1,0,8], [102,1,1,7], [102,1,2,8]]
    @vc2.remote_sender <+ [[102,1,0]]
    @vc2.tick
    set_assert_equal([[0,0,8], [0,1,8], [0,2,8]], @vc2.event_clock.to_a)

    @vc2.remote_signal <+ [[103,3,0,8], [103,3,3,1]]
    @vc2.remote_sender <+ [[103,3,0]]
    @vc2.tick
    set_assert_equal([[0,0,9], [0,3,2], [0,1,8], [0,2,8]], 
                     @vc2.event_clock.to_a)
    
    # Testing simultaneous updates
    @vc2.remote_signal <+ [[104,3,2,11], [104,3,3,5], [104,3,1,11], 
                           [104,2,2,11], [104,2,1,9]]
    @vc2.remote_sender <+ [[104,3,0], [104,2,0]]
    @vc2.tick
    set_assert_equal([[0,0,10], [0,3,6], [0,1,11], [0,2,12]], 
                     @vc2.event_clock.to_a)

  end

  def test_c_ordering
    p "test c ordering"
    @vc3.vc_x <+ [[0,0,1], [0,1,2], [0,2,4]]
    @vc3.vc_y <+ [[0,0,9], [0,1,8], [0,2,8]]
    @vc3.tick
    set_assert_equal([[0,-1]], @vc3.order.to_a)

    @vc3.vc_y <+ [[0,0,1], [0,1,2], [0,2,4]]
    @vc3.vc_x <+ [[0,0,9], [0,1,8], [0,2,8]]
    @vc3.tick
    set_assert_equal([[0,1]], @vc3.order.to_a)

    @vc3.vc_y <+ [[0,0,1], [0,1,2], [0,2,4]]
    @vc3.vc_x <+ [[0,0,1], [0,3,8]]
    @vc3.tick
    set_assert_equal([[0,0]], @vc3.order.to_a)

    @vc3.vc_x <+ [[0,0,9], [0,3,2], [0,1,8], [0,2,8], 
                  [1,0,10], [1,3,7], [1,1,8], [1,2,8]]
    @vc3.vc_y <+ [[0,0,10], [0,3,6], [0,1,8], [0,2,8], 
                  [1,0,9], [1,3,2], [1,1,8], [1,2,8]]
    @vc3.tick
    set_assert_equal([[0,-1],[1,1]], @vc3.order.to_a)
  end

  def test_d_serialize
    p "test d serialize"
    @vc4.serialize <+ [[0,0,0,1], [0,0,1,2], [0,0,2,4],
                       [1,0,0,10], [1,0,3,6], [1,0,1,11], [1,0,2,12]]
    @vc4.tick
    set_assert_equal([[0, [[0,0,1], [0,1,2], [0,2,4]]],
                      [1, [[0,0,10], [0,1,11], [0,2,12], [0,3,6]]]], 
                     @vc4.serialize_rs.to_a.map do |x|
                       [x[0]] + [x[1].sort]
                     end)
    
    @vc4.deserialize <+ @vc4.serialize_rs
    @vc4.tick
    set_assert_equal([[0,0,0,1], [0,0,1,2], [0,0,2,4],
                       [1,0,0,10], [1,0,3,6], [1,0,1,11], [1,0,2,12]],
                     @vc4.deserialize_rs.to_a)
  end
end
