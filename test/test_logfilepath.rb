require 'test/unit'
require 'bricolage/logfilepath'
require 'bricolage/jobnet'

module Bricolage
  class TestLogFilePath < Test::Unit::TestCase
    test '#format' do
      path = LogFilePath.new('./log/%{std}.log')
      ref = JobNet::JobRef.new('subsys', 'somejob', '-')
      start = Time.new(2012,3,4,12,34,56)
      assert_equal './log/20120304/subsys::rebuild/20120304_123456000/subsys-somejob.log', path.format(
        job_ref: ref,
        jobnet_id: 'subsys/rebuild',
        job_start_time: start,
        jobnet_start_time: start
      )
    end
  end
end
