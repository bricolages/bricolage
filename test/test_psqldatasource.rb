require 'test/unit'
require 'bricolage/psqldatasource'

module Bricolage
  class TestPSQLDataSource < Test::Unit::TestCase
    test "#open" do
      @ctx = Context.for_application('.', environment: 'test')
      ds = @ctx.get_data_source('sql', 'test_db')
      connection = nil
      ds.open {|conn|
        assert_false conn.closed?
        connection = conn
      }
      assert_true connection.closed?
    end

    test "load option is correctly formatted" do
      assert_equal 'gzip', PSQLLoadOptions::Option.new('gzip', true).to_s
      assert_equal "json 'auto'", PSQLLoadOptions::Option.new('json', 'auto').to_s
      assert_equal 'encoding utf16le', PSQLLoadOptions::Option.new('encoding', 'utf16le').to_s
    end
  end
end
