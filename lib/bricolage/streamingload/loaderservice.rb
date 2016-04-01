require 'bricolage/streamingload/loader'
require 'bricolage/streamingload/loadtask'
require 'bricolage/streamingload/loadparams'
require 'bricolage/exception'
require 'bricolage/version'
require 'optparse'

module Bricolage

  module StreamingLoad

    class LoaderService

      def LoaderService.main
require 'pp'
require 'pry'
        opts = LoaderServiceOptions.new(ARGV)
        config_path, task_id = opts.parse

        config = YAML.load(File.read(config_path))
        ctx = Context.for_application('.')
        loader = new(
          context: ctx,
          data_source: ctx.get_data_source('sql', config['redshift-ds']),
          logger: ctx.logger
        )
        loader.execute_task task_id
      end

      def initialize(context:, data_source:, logger:)
        @ctx = context
        @ds = data_source
        @logger = logger
      end

      def execute_task(task_id)
        task = @ds.open {|conn| LoadTask.load(conn, task_id) }
        params = LoaderParams.load(@ctx, task)
        loader = Loader.new(@ctx, logger: @ctx.logger)
        loader.process(task, params)
      end

    end


    class LoaderServiceOptions

      def initialize(argv)
        @argv = argv
        @rest_arguments = nil
        @opts = opts = OptionParser.new
        opts.on('--task-id=ID', 'Execute oneshot load task (implicitly disables daemon mode).') {|task_id|
          @task_id = task_id
        }
        opts.on('--help', 'Prints this message and quit.') {
          puts opts.help
          exit 0
        }
        opts.on('--version', 'Prints version and quit.') {
          puts "#{File.basename($0)} version #{VERSION}"
          exit 0
        }
      end

      attr_reader :task_id
      attr_reader :rest_arguments

      def parse
        @opts.parse!(@argv)
        @rest_arguments = @argv.dup
      rescue OptionParser::ParseError => err
        raise OptionError, err.message
      end

    end

  end

end
