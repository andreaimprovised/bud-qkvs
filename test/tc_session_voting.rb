require 'rubygems'
require 'bud'
require 'test/unit'
require 'session_guarantees/session_guarantee_voting'


class SessionVoter
  include Bud
  include SessionVoteCounter

  bloom do
#   stdio <~ read_vectors.inspected
#   stdio <~ write_vector.inspected
#   stdio <~ output_write_result.inspected
   stdio <~ output_read_result.inspected
  end
end

class TestSessionVoting < Test::Unit::TestCase

  def wait
    2.times do
      @voter.tick
    end
  end

  def init_dummy_request
    @voter.init_request <+ [[0, [:foo, :bar], [[['a', 0]], [['b', 1]]], [['a', 1]]]]
  end

  def setup
    @voter = SessionVoter.new()
  end

  def test_init_guarantees
    p 'test_init_guarantees'
    init_dummy_request
    wait
    assert(@voter.session_guarantees.include?([0, [:foo, :bar]]))
  end

  def test_init_read_vectors
    p 'test_init_read_vectors'
    init_dummy_request
    wait
    assert(@voter.read_vectors.include?([0, [['a', 0]]]))
    assert(@voter.read_vectors.include?([0, [['b', 1]]]))
  end

  def test_init_write_vector
    p 'test_init_write_vectors'
    init_dummy_request
    wait
    assert(@voter.write_vector.include?([0, [['a', 1]]]))
  end

  def test_monotonic_reads_empty
    p 'test_monotonic_reads_empty'
    @voter.init_request <+ [[0, [:MR], [[['a', 0]]], [['a', 1]]]]
    wait
    assert(@voter.output_read_result.empty?)
  end

  def test_read_your_writes_empty
    p 'test_read_your_writes_empty'
    @voter.init_request <+ [[0, [:RYW], [[['a', 0]]], [['a', 1]]]]
    wait
    assert(@voter.output_read_result.empty?)
  end

  def test_writes_follow_reads_empty
    p 'test_writes_follow_reads_empty'
    @voter.init_request <+ [[0, [:WFR], [[['a', 0]]], [['a', 1]]]]
    wait
    assert(@voter.output_write_result.include?([0, [['a', 0]]]))
  end

  def test_monotonic_writes_empty
    p 'test_monotonic_writes_empty'
    @voter.init_request <+ [[0, [:MW], [['a', 0]], [['a', 1]]]]
    wait
    assert(@voter.output_write_result.include?([0, [['a', 1]]]))
  end

  def test_read_simple
    p 'test_read_simple'
    @voter.init_request <+ [[0, [], [[]], []]]
    wait
    @voter.add_read <+ [[0, [['a', 1]], 'VALUEA1']]
    wait
    assert(@voter.output_read_result.include?([0, [['a', 1]], 'VALUEA1']))
  end

  def test_read_domination
    p 'test_read_domination'
    @voter.init_request <+ [[0, [], [[]], []]]
    wait
    @voter.add_read <+ [[0, [['a', 1]], 'VALUEA1']]
    @voter.add_read <+ [[0, [['a', 2]], 'VALUEA2']]
    wait
    assert(@voter.output_read_result.include?([0, [['a', 2]], 'VALUEA2']))
  end

  def test_read_conflict
    p 'test_read_conflict'
    @voter.init_request <+ [[0, [], [[]], []]]
    wait
    @voter.add_read <+ [[0, [['a', 1]], 'VALUEA1']]
    @voter.add_read <+ [[0, [['b', 2]], 'VALUEA2']]
    wait
    assert(@voter.output_read_result.include?([0, [['a', 1]], 'VALUEA1']))
    assert(@voter.output_read_result.include?([0, [['b', 2]], 'VALUEA2']))
  end

end
