require 'rubygems'
require 'bud'
require 'backports'
require 'test/unit'
require 'read_repair/read_repair'

class TestReadRepair < Test::Unit::TestCase

  class RRepair
    include Bud
    include ReadRepair
  end

  def setup
    @RR = RRepair.new
  end

  def teardown
  
  end

  def test_RR
		p 'dominance tests'
		@RR.read_requests <+ [[0, 'k1', 'a']]
		w_req = @RR.sync_callback(@RR.read_acks.tabname, [[0, [['a', 3], ['b', 3]], ['high']], [0, [['a', 2], ['b', 2]], ['low']]], @RR.write_requests.tabname)
		assert_equal([w_req[0][0], w_req[0][1], w_req[0][2].sort, w_req[0][3].sort], [0, 'k1', [['a', 4], ['b', 3]], ['high']])

		@RR.read_requests <+ [[1, 'k2', 'b']]
		w_req = @RR.sync_callback(@RR.read_acks.tabname, [[1, [['a', 7], ['b', 9]], ['cabbage']], [1, [['a', 6]], ['lettuce']]], @RR.write_requests.tabname)
		assert_equal([w_req[0][0], w_req[0][1], w_req[0][2].sort, w_req[0][3].sort], [1, 'k2', [['a', 7], ['b', 10]], ['cabbage']])

    p 'concurrency tests'
		@RR.read_requests <+ [[0, 'k3', 'c']]
		w_req = @RR.sync_callback(@RR.read_acks.tabname, [[0, [['a', 3], ['b', 5]], ['start']], [0, [['a', 3], ['c', 5]], ['go']]], @RR.write_requests.tabname)
		assert_equal([w_req[0][0], w_req[0][1], w_req[0][2].sort, w_req[0][3].sort], [0, 'k3', [['a', 3], ['b', 5], ['c', 6]], ['go', 'start']])

		@RR.read_requests <+ [[1, 'k4', 'a']]
		w_req = @RR.sync_callback(@RR.read_acks.tabname, [[1, [['a', 3], ['b', 5]], ['cool']], [1, [['a', 4], ['b', 4]], ['school']]], @RR.write_requests.tabname)
		assert_equal([w_req[0][0], w_req[0][1], w_req[0][2].sort, w_req[0][3].sort], [1, 'k4', [['a', 5], ['b', 5]], ['cool', 'school']])

    @RR.read_requests <+ [[2, 'k5', 'a']]
  	w_req = @RR.sync_callback(@RR.read_acks.tabname, [[2, [['a', 5], ['b', 1], ['c', 2], ['d', 9]], ['foo','poo']],[2, [['a', 2], ['b', 6], ['c', 1]], ['you']], [2, [['a', 5], ['b', 6], ['c', 1], ['d', 9]], ['goo']], [2, [['b', 1], ['c', 0], ['d', 0]], ['shoe']],[2, [['a', 2], ['b', 7], ['c', 1]], ['clue']]], @RR.write_requests.tabname)
		assert_equal([w_req[0][0], w_req[0][1], w_req[0][2].sort, w_req[0][3].sort], [2, 'k5', [['a', 6], ['b', 7], ['c', 2], ['d', 9]], ['clue', 'foo', 'goo', 'poo']])

		p 'mixed requests tests'
		@RR.read_requests <+ [[0, 'k6', 'a']]
		@RR.read_requests <+ [[1, 'k7', 'c']]
		w_req = @RR.sync_callback(@RR.read_acks.tabname, [[0, [['a', 3], ['b', 4]], ['start']], [0, [['a', 3], ['b', 5]], ['go']], [1, [['c', 2], ['d', 4]], ['cabbage']], [1, [['c', 3], ['d', 2]], ['lettuce']]], @RR.write_requests.tabname)
		p w_req
		if w_req[0][0] == 0
			assert_equal([w_req[0][0], w_req[0][1], w_req[0][2].sort, w_req[0][3].sort], [0, 'k6', [['a', 4], ['b', 5]], ['go']])
			assert_equal([w_req[1][0], w_req[1][1], w_req[1][2].sort, w_req[1][3].sort], [1, 'k7', [['c', 4], ['d', 4]], ['cabbage', 'lettuce']])
		else
			assert_equal([w_req[1][0], w_req[1][1], w_req[1][2].sort, w_req[1][3].sort], [0, 'k6', [['a', 4], ['b', 5]], ['go']])
			assert_equal([w_req[0][0], w_req[0][1], w_req[0][2].sort, w_req[0][3].sort], [1, 'k7', [['c', 4], ['d', 4]], ['cabbage', 'lettuce']])
		end

  end
end
