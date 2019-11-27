require 'test/unit'
require 'bricolage/jobclass'
Bricolage::JobClass.get('streaming_load')

module Bricolage
  class TestStreamingLoadJobClass_S3Queue < Test::Unit::TestCase
    def test_compile_name_pattern
      q = StreamingLoadJobClass::S3Queue.new(data_source: nil, ctl_ds: nil, queue_path: nil, ctl_prefix: nil, persistent_path: nil, file_name: nil, logger: nil)
      re = q.compile_name_pattern("%*%Y%m%d-%H%M_%Q.gz")
      assert_equal /\A[^\/]*(?<year>\d{4})(?<month>\d{2})(?<day>\d{2})\-(?<hour>\d{2})(?<minute>\d{2})_(?<seq>\d+)\.gz\z/, re
    end
  end
end
