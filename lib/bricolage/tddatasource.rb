require 'bricolage/datasource'
require 'bricolage/commandutils'
require 'stringio'
require 'date'

module Bricolage

  class TDDataSource < DataSource
    declare_type 'td'

    include CommandUtils

    def initialize(database: nil, username: nil, apikey: nil, td: 'td', priority: -2)
      @database = database
      @apikey = apikey
      @td = td
      @priority = priority
    end

    def new_task
      TDTask.new(self)
    end

    def query_command_args(*args)
      [@td, "--apikey=#{@apikey}", "query", "--database=#{@database}", "--wait"] + args
    end

    def delete_command_args(*args)
      [@td, "--apikey=#{@apikey}", "table:partial_delete", @database, *args, "--wait"]
    end

    def exec(*args)
      JobResult.for_process_status(command(*args))
    end
  end

  class TDTask < DataSourceTask
    def export(stmt, path: nil, **opts)
      add TDTask::Export.new(stmt, path: path, **opts)
    end

    class Export < Action
      include CommandUtils

      def initialize(stmt, path: nil, format: nil, gzip: false, override: false)
        @statement = stmt
        @path = path
        @format = format
        @gzip = gzip
        @override = override
      end

      def_delegator '@statement', :bind

      def source
        buf = StringIO.new
        buf.puts command_args(new_tmpfile_path).join(' ') + " <<EndTDSQL"
        buf.puts @statement.stripped_source
        buf.puts 'EndTDSQL'
        buf.string
      end

      GZIP_COMMAND = 'gzip'   # FIXME: parameterize

      def run
        if File.exist?(@path) and not @override
          raise JobFailure, "target file exists: #{@path.inspect}"
        end
        puts @statement.source
        td_result = make_tmpfile(@statement.stripped_source) {|query_path|
          ds.exec(*command_args(query_path))
        }
        if @gzip
          gzip_result = ds.command(GZIP_COMMAND, export_path)
          raise JobFailure, "gzip failed" unless gzip_result.success?
        end
        td_result
      end

      def command_args(query_path)
        ds.query_command_args("--query=#{query_path}", "--output=#{export_path}", "--format=#{@format}")
      end

      def export_path
        @gzip ? @path.sub(/\.gz\z/, '') : @path
      end
    end

    def delete(table_name, **opts)
      add TDTask::Delete.new(table_name, **opts)
    end

    class Delete < Action
      def initialize(table_name, from:, to:)
        @table = table_name
        @from = from.to_time.to_i
        @to = to.to_time.to_i
      end

      def run
        td_result = ds.exec(source)
      end

      def command_args
        ds.delete_command_args(@table, "--from #{@from}", "--to #{@to}")
      end

      def source
        buf = StringIO.new
        buf.puts command_args.join(' ')
        buf.string
      end
    end
  end

end
