require 'rubygems'
require 'bud'
require 'test/unit'
require 'session_guarantees/session_guarantee_voting'


class SessionVoter
  include Bud
  include SessionVoteCounter

  bloom do
    stdio <~ output_read_result.inspected
    stdio <~ output_write_result.inspected
  end
end

class TestSessionVoting < Test::Unit::TestCase

  def setup
    @voter = SessionVoter.new()
    @voter.run_bg
  end

  def teardown
    @voter.stop
  end

  def test_sanity
    p 'test_sanity'
  end

end
