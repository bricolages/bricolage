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
        opts = LoaderServiceOptions.new(ARGV)
        opts.parse
        unless opts.rest_arguments.size == 1
          $stderr.puts opts.usage
          exit 1
        end
        config_path, * = opts.rest_arguments

        config = YAML.load(File.read(config_path))

        ctx = Context.for_application('.')
        redshift_ds = ctx.get_data_source('sql', config.fetch('redshift-ds'))
        task_queue = ctx.get_data_source('sqs', config.fetch('task-queue-ds'))

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

      def handle_streaming_load_v3(task)
        # FIXME: check initialized/disabled
        @logger.info "handling load task: table=#{task.qualified_name} task_id=#{task.id} task_seq=#{task.seq}"
        loader = Loader.load_from_file(@ctx, task, logger: @ctx.logger)
        loader.execute
        @task_queue.delete_message(task)
      end

    end


    class LoaderServiceOptions

      def initialize(argv)
        @argv = argv
        @task_id = nil
        @daemon = false
        @rest_arguments = nil

        @opts = opts = OptionParser.new("Usage: #{$0} CONFIG_PATH")
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

      def usage
        @opts.help
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
