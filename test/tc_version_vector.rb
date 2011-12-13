require 'rubygems'
require 'bud'
require 'backports'
require 'test/unit'
require 'causality/version_vector'


class TestVersionVectors < Test::Unit::TestCase

  def set_eq(goal, actual)
    assert_equal(goal.to_a.sort, actual.to_a.sort)
  end

  class VVSerializer
    include Bud
    include VersionVectorSerializer
  end

  class VVMSerializer
    include Bud
    include VersionMatrixSerializer
  end

  class VVMerger
    include Bud
    include VersionVectorMerge
  end

  class VVDomination
    include Bud
    include VersionVectorDomination
  end

  class VVConcurrency
    include Bud
    include VersionVectorConcurrency
  end

  class VVKVS
    include Bud
    include VersionVectorKVS
  end

  def setup
    @a = VVSerializer.new
    @b = VVMerger.new
    @c = VVDomination.new
    @d = VVConcurrency.new
    @e = VVKVS.new
    @f = VVMSerializer.new
  end

  def teardown

  end

  def test_a_serialize
    p 'serialize test'
    @a.serialize <+ [[0, 'a', 0], [0, 'b', 1],
                     [0, 'c', 2],
                     [1, 'a', 2], [1, 'b', 0],
                     [1, 'c', 1]]
    @a.tick
    res = @a.serialize_ack.to_a.sort
    assert_equal(0, res[0][0])
    set_eq([['a', 0], ['b', 1], ['c', 2]],
           res[0][1])
    assert_equal(1, res[1][0]) 
    set_eq([['a', 2], ['b', 0], ['c', 1]],
           res[1][1])

    @a.deserialize <+ [[0, [['a', 0], ['b', 1], ['c', 2]]], 
                       [1, [['a', 2], ['b', 0], ['c', 1]]]]
    @a.tick
    set_eq([[0, 'a', 0], [0, 'b', 1],
            [0, 'c', 2],
            [1, 'a', 2], [1, 'b', 0],
            [1, 'c', 1]], 
           @a.deserialize_ack) 
  end

  def test_b_merge
    p 'merge test'
    @b.version_matrix <+ [[0, [['a', 5], ['b', 1], ['c', 2], ['d', 9]]], 
                          [0, [['a', 2], ['b', 6], ['c', 1]          ]],
                          [1, [          ['b', 1], ['c', 2], ['d', 9]]], 
                          [1, [['a', 2], ['b', 4], ['c', 1]          ]]]
    @b.tick
    res = @b.merge_vector.to_a.sort
    assert_equal(0, res[0][0])
    set_eq([['a', 5], ['b', 6], ['c', 2], ['d', 9]],
           res[0][1])
    assert_equal(1, res[1][0])
    set_eq([['a', 2], ['b', 4], ['c', 2], ['d', 9]],
           res[1][1])
  end

  def test_c_domination
    p "domination test"
    @c.domination_query <+ [[0, 
                             [['a', 5], ['b', 1], ['c', 2], ['d', 9]], 
                             [['a', 2], ['b', 6], ['c', 1]          ]]]
    @c.domination_query <+ [[1, 
                             [['a', 5], ['b', 6], ['c', 2], ['d', 9]], 
                             [['a', 2], ['b', 6], ['c', 1]          ]]]
    @c.domination_query <+ [[2, 
                             [          ['b', 1], ['c', 0], ['d', 0]], 
                             [['a', 2], ['b', 6], ['c', 1]          ]]]
    @c.tick
    set_eq([[0, 0], [1, 1], [2, -1]], @c.result)
  end

  def test_d_concurrency
    p "concurrency test"
    @d.version_matrix <+ [[0, [['a', 5], ['b', 1], ['c', 2], ['d', 9]]], 
                          [0, [['a', 2], ['b', 6], ['c', 1]          ]],
                          [0, [['a', 5], ['b', 6], ['c', 1], ['d', 9]]], 
                          [0, [          ['b', 1], ['c', 0], ['d', 0]]],
                          [0, [['a', 2], ['b', 7], ['c', 1]          ]]]
    @d.tick
    set_eq([[0, [['a', 5], ['b', 1], ['c', 2], ['d', 9]]], 
            [0, [['a', 5], ['b', 6], ['c', 1], ['d', 9]]], 
            [0, [['a', 2], ['b', 7], ['c', 1]          ]]],
           @d.minimal_matrix)
    
  end

  def test_e_kvs
    p "kvs test"
    @e.write <+ [[0, 'foo', [['a',1], ['b', 1]], 'bar'],
                 [1, 'foo', [['a',2], ['b', 1]], 'kag'],
                 [2, 'foo', [['a',1], ['b', 2]], 'zig']]
    @e.tick
    @e.tick
    set_eq([[2, [["a", 1], ["b", 2]], "zig"], 
               [2, [["a", 2], ["b", 1]], "kag"], 
               [1, [["a", 2], ["b", 1]], "kag"], 
               [0, [["a", 1], ["b", 2]], "zig"], 
               [1, [["a", 1], ["b", 2]], "zig"], 
               [0, [["a", 2], ["b", 1]], "kag"]],
              @e.write_ack)

    @e.read <+ [[3, 'foo']]
    @e.tick
    set_eq([[3, [["a", 1], ["b", 2]], "zig"], 
            [3, [["a", 2], ["b", 1]], "kag"]],
           @e.read_ack)
  end

  def test_f_m_serializer
    p "matrix serialization"
    @f.serialize <+ [[0, [['a', 0], ['b', 1], ['c', 2]]], 
                     [0, [['a', 2], ['b', 0], ['c', 1]]]]
    @f.tick
    p @f.serialize_ack.to_a
    @f.deserialize <+ [[0, [[['a', 0], ['b', 1], ['c', 2]],
                           [['a', 2], ['b', 0], ['c', 1]]]
                       ]]
    @f.tick
    p @f.deserialize_ack.to_a
  end
end
