require 'test/unit'
require 'bricolage/filesystem'
require 'pathname'
require 'pp'

module Bricolage
  class TestFileSystem < Test::Unit::TestCase
    fs = FileSystem.new("#{__dir__}/home", "test")
    subfs = fs.subsystem('subsys')

    test "FileSystem.job_file" do
      path = subfs.job_file('unified')
      assert_instance_of Pathname, path
      assert_equal subfs.relative('unified.sql.job'), path

      assert_equal subfs.relative('separated.job'), subfs.job_file('separated')
    end
  end
end
