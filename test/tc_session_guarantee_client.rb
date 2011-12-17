require 'rubygems'
require 'bud'
require 'test/unit'
require 'session_guarantees/session_guarantee_client'

class TestClient
  include Bud
  include SessionQuorumKVSClient
end

class TestQClient < Test::Unit::TestCase

  def setup()
    @tc = TestClient.new
    @tc.run_bg
    @tc.create_session <+ [[0, 'mr']]
    @tc.tick
    @tc.create_session <+ [[1, 'mw']]
    @tc.tick
  end

  def teardown()
  end


  def test_create_session
    assert_equal([1,'mr'], @tc.sessions[[1]])
    assert_equal([2,'mw'], @tc.sessions[[2]])
  end

  def test_kvget
    @tc.kvget <+ [[1, 2, 'testkey']]
    @tc.tick
    assert_equal([2, 'testkey', 'mr', [], []], @tc.kvread.first)
  end

  def test_kvput
    @tc.kvput <+ [[1, 2, 'testkey', 'testvalue']]
    @tc.tick
    assert_equal([2, 'testkey', 'testvalue', 'mr', [], []], @tc.kvwrite.first)
  end

  def test_kvdel
    @tc.kvdel <+ [[2, 2, 'testkey']]
    @tc.tick
    assert_equal([2, 'testkey', nil, 'mw', [], []], @tc.kvwrite.first)
  end


end
