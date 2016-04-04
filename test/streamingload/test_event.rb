require 'test/unit'
require 'bricolage/streamingload/event'

module Bricolage::StreamingLoad

  class TestEvent < Test::Unit::TestCase

    def new_s3event(message_id: nil, receipt_handle: nil, name: nil, time: nil, region: nil, bucket: nil, key: nil, size: nil)
      S3ObjectEvent.new(
        message_id: message_id,
        receipt_handle: receipt_handle,
        name: name,
        time: time,
        region: region,
        bucket: bucket,
        key: key,
        size: size
      )
    end

    test "#created?" do
      e = new_s3event(name: "ObjectCreated:Put")
      assert_true e.created?
      e = new_s3event(name: "ObjectCreated:Copy")
      assert_false e.created?
    end

  end

end
