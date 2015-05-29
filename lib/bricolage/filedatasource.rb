require 'bricolage/datasource'
require 'stringio'

module Bricolage

  class FileDataSource < DataSource
    declare_type 'file'

    def initialize(opts)
    end

    def new_task
      FileTask.new(self)
    end
  end

  class FileTask < DataSourceTask
    def remove(src)
      add Remove.new(src)
    end

    class Remove < Action
      def initialize(src)
        @src = src
      end

      def source_files
        Dir.glob(@src)
      end

      def source
        "rm -f #{@src}"
      end

      def run
        FileUtils.rm_f source_files, verbose: true
        nil
      end
    end
  end

end
