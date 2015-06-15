require 'bricolage/vacuumlock'

module Bricolage

  module EmbeddedCodeAPI
    private

    def user_home
      Pathname(ENV['HOME'])
    end

    def user_home_relative_path(rel)
      user_home + rel
    end

    def app_home_relative_path(rel)
      app_home + rel
    end

    def relative_path(rel)
      base_dir + rel
    end

    def read_file_if_exist(path)
      return nil unless File.exist?(path)
      File.read(path)
    end

    def date(str)
      Date.parse(str)
    end

    def ymd(date)
      date.strftime('%Y-%m-%d')
    end

    def attribute_tables(attr)
      all_tables.select {|table| table.attributes.include?(attr) }
    end

    def all_tables
      Dir.glob("#{app_home}/*/*.ct").map {|path|
        SQLStatement.new(FileResource.new(path))
      }
    end

    include VacuumLock
  end

end
