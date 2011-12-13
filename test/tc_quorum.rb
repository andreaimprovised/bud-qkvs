require 'rubygems'
require 'bud'
require 'test/unit'
require 'quorum/quorum'

class TestQuorum < Test::Unit::TestCase

  class QuorumDude
    include Bud
    include QuorumAgent

    bloom do
      stdio <~ write_ack.inspected
      stdio <~ read_ack.inspected
      stdio <~ version_ack.inspected
    end
  end

  def assert_set_equal(goal, actual)
    
    assert_equal(goal.to_a.sort, actual.to_a.sort)

  end

  def setup
    @a = QuorumDude.new(:ip => '127.0.0.1',
                        :port => '8080')
    @a_o1 = QuorumDude.new(:ip => '127.0.0.1',
                           :port => '8081')
    @a.add_member <+ [['127.0.0.1:8080', 0],
                      ['127.0.0.1:8081', 1]]
    @a_o1.add_member <+ [['127.0.0.1:8080', 0],
                         ['127.0.0.1:8081', 1]]
    @a.run_bg
    @a_o1.run_bg
  end

  def teardown
    @a.stop
    @a_o1.stop
  end

  def test_a
    res = @a.sync_callback(:write, 
                           [[0, 'foo', [[0,1],[0,0]], 'shamalama'],
                            [1, 'jaz', [[0,1],[0,0]], 'le big mac']],
                           :write_ack)
    sleep(1)
    res = @a.sync_callback(:read, [[2, 'foo'], [3, 'jaz']], :read_ack)
    sleep(1)
    res = @a.sync_callback(:version_query, \
                           [[4, 'foo'], [5, 'jaz']], :version_ack)
    sleep(1)
  end

end
