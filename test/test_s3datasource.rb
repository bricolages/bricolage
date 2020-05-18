require 'test/unit'
require 'mocha/test_unit'
require 'bricolage/s3datasource'
require 'bricolage/logger'
require 'pp'
require 'aws-sdk-s3'

module Bricolage
  class TestS3datasouce < Test::Unit::TestCase

    def setup
      @ds = S3DataSource.new
      @ds.stubs(:logger).returns(Logger.new)
    end

    test "traverse without exception" do
      bucket = mock()
      bucket.stubs(:objects).returns(['prefix/a'])
      @ds.stubs(:bucket).returns(bucket)
      assert_equal ['prefix/a'], @ds.traverse('prefix')
    end

    test "traverse with 2 exception" do
      bucket = mock()
      bucket.stubs(:objects).raises(Aws::Xml::Parser::ParsingError.new("test message","0","test column")).then.
        raises(Aws::Xml::Parser::ParsingError.new("test message","0","test column")).then.returns(['prefix/a'])
      @ds.stubs(:bucket).returns(bucket)
      assert_equal ['prefix/a'], @ds.traverse('prefix')
    end

    test "traverse with more than 3 exception" do
      bucket = mock()
      bucket.stubs(:objects).raises(Aws::Xml::Parser::ParsingError.new("test message","0","test column")).then.
        raises(Aws::Xml::Parser::ParsingError.new("test message","0","test column")).then.
        raises(Aws::Xml::Parser::ParsingError.new("test message","0","test column")).then.
        raises(Aws::Xml::Parser::ParsingError.new("test message","0","test column")).then.returns([])
      @ds.stubs(:bucket).returns(bucket)
      assert_raise(Aws::Xml::Parser::ParsingError) { @ds.traverse(nil) }
    end
  end
end
