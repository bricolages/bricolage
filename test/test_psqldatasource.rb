require 'test/unit'
require 'bricolage/psqldatasource'

module Bricolage
  class TestPSQLLoadOptions < Test::Unit::TestCase
    test "load option is correctly formatted" do
      assert_equal 'gzip', PSQLLoadOptions::Option.new('gzip', true).to_s
      assert_equal "json 'auto'", PSQLLoadOptions::Option.new('json', 'auto').to_s
      assert_equal 'encoding utf16le', PSQLLoadOptions::Option.new('encoding', 'utf16le').to_s
    end
  end
end
