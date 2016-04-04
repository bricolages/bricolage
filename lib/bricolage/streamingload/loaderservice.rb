require 'bricolage/sqsdatasource'
require 'bricolage/streamingload/task'
require 'bricolage/streamingload/loader'
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
        unless ARGV.size == 1
        config_path = opts.parse

        config = YAML.load(File.read(config_path))

        ctx = Context.for_application('.')
        redshift_ds = ctx.get_data_source('sql', config['redshift-ds']),
        task_queue = ctx.get_data_source('sqs', config['task-queue-ds'])

        service = new(
          context: ctx,
          data_source: redshift_ds,
          task_queue: task_queue,
          logger: ctx.logger
        )

        if opts.task_id
          # Single task mode
          service.execute_task opts.task_id
        else
          # Server mode
          # FIXME: handle --daemon
          service.main_loop
        end
      end

      def initialize(context:, data_source:, task_queue: nil, logger:)
        @ctx = context
        @ds = data_source
        @task_queue = task_queue
        @logger = logger
      end

      def execute_task(task_id)
        task = @ds.open {|conn| LoadTask.load(conn, task_id) }
        loader = Loader.load_from_file(@ctx, task, logger: @ctx.logger)
        loader.execute
      end

      def main_loop
        @task_queue.main_handler_loop(handlers: self, message_class: Task)
      end

      def handle_load(task)
        # FIXME: check initialized/disabled
        loader = Loader.load_from_file(@ctx, task, logger: @ctx.logger)
        loader.execute
      end

    end


    class LoaderServiceOptions

      def initialize(argv)
        @argv = argv
        @task_id = nil
        @daemon = false
        @rest_arguments = nil

        @opts = opts = OptionParser.new
        opts.on('--task-id=ID', 'Execute oneshot load task (implicitly disables daemon mode).') {|task_id|
          @task_id = task_id
        }
        opts.on('--daemon', 'Becomes daemon in server mode.') {
          @daemon = true
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
