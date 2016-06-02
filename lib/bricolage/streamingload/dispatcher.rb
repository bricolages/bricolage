require 'bricolage/exception'
require 'bricolage/version'
require 'bricolage/sqsdatasource'
require 'bricolage/streamingload/event'
require 'bricolage/streamingload/objectbuffer'
require 'bricolage/streamingload/urlpatterns'
require 'aws-sdk'
require 'yaml'
require 'optparse'
require 'fileutils'

module Bricolage

  module StreamingLoad

    class Dispatcher

      def Dispatcher.main
        opts = DispatcherOptions.new(ARGV)
        opts.parse
        unless opts.rest_arguments.size == 1
          $stderr.puts opts.usage
          exit 1
        end
        config_path, * = opts.rest_arguments
        set_log_path opts.log_file_path if opts.log_file_path

        config = YAML.load(File.read(config_path))
        ctx = Context.for_application('.', environment: opts.environment)
        event_queue = ctx.get_data_source('sqs', config.fetch('event-queue-ds'))
        task_queue = ctx.get_data_source('sqs', config.fetch('task-queue-ds'))

        object_buffer = ObjectBuffer.new(
          task_queue: task_queue,
          data_source: ctx.get_data_source('sql', 'sql'),
          control_data_source: ctx.get_data_source('s3', config.fetch('ctl-ds')),
          default_buffer_size_limit: 500,
          default_load_interval: 300,
          process_flush_interval: 60,
          context: ctx
        )

        url_patterns = URLPatterns.for_config(config.fetch('url_patterns'))

        dispatcher = Dispatcher.new(
          event_queue: event_queue,
          object_buffer: object_buffer,
          url_patterns: url_patterns,
          logger: ctx.logger
        )

        Process.daemon(true) if opts.daemon?
        create_pid_file opts.pid_file_path if opts.pid_file_path
        dispatcher.set_processflush_timer
        dispatcher.event_loop
      end

      def Dispatcher.set_log_path(path)
        FileUtils.mkdir_p File.dirname(path)
        # make readable for retrieve_last_match_from_stderr
        File.open(path, 'w+') {|f|
          $stdout.reopen f
          $stderr.reopen f
        }
      end

      def Dispatcher.create_pid_file(path)
        File.open(path, 'w') {|f|
          f.puts $$
        }
      rescue
        # ignore
      end

      def initialize(event_queue:, object_buffer:, url_patterns:, logger:)
        @event_queue = event_queue
        @object_buffer = object_buffer
        @url_patterns = url_patterns
        @logger = logger
      end

      def event_loop
        @event_queue.main_handler_loop(handlers: self, message_class: Event)
      end

      def handle_shutdown(e)
        @event_queue.initiate_terminate
        @event_queue.delete_message(e)
      end

      def handle_data(e)
        unless e.created?
          @event_queue.delete_message(e)
          return
        end
        obj = e.loadable_object(@url_patterns)
        buf = @object_buffer[obj.qualified_name]
        unless buf
          @event_queue.delete_message(e)
          return
        end
        if buf.empty?
          set_flush_timer obj.qualified_name, buf.load_interval
        end
        buf.put(obj)
      end

      def set_flush_timer(table_name, sec)
        @event_queue.send_message FlushEvent.create(table_name: table_name, delay_seconds: sec)
      end

      def handle_flush(e)
        # might be nil in rare case ( stop -> del job file -> start )
        @object_buffer[e.table_name].request_flush if @object_buffer[e.table_name]
        @event_queue.delete_message(e)
      end

      def handle_processflush(e)
        @event_queue.delete_message(e)
        load_tasks = @object_buffer.process_flush
        load_tasks.each {|load_task| delete_events(load_task.source_events) }
        set_processflush_timer
      end

      def set_processflush_timer
        @event_queue.send_message ProcessFlushEvent.create(delay_seconds: @object_buffer.process_flush_interval)
      end

      def delete_events(events)
        events.each do |e|
          @event_queue.delete_message(e)
        end
      end

    end


    class DispatcherOptions

      def initialize(argv)
        @argv = argv
        @daemon = false
        @log_file_path = nil
        @pid_file_path = nil
        @rest_arguments = nil

        @opts = opts = OptionParser.new("Usage: #{$0} CONFIG_PATH")
        opts.on('--task-id=ID', 'Execute oneshot load task (implicitly disables daemon mode).') {|task_id|
          @task_id = task_id
        }
        opts.on('-e', '--environment=NAME', "Sets execution environment [default: #{Context::DEFAULT_ENV}]") {|env|
          @environment = env
        }
        opts.on('--daemon', 'Becomes daemon in server mode.') {
          @daemon = true
        }
        opts.on('--log-file=PATH', 'Log file path') {|path|
          @log_file_path = path
        }
        opts.on('--pid-file=PATH', 'Creates PID file.') {|path|
          @pid_file_path = path
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

      def parse
        @opts.parse!(@argv)
        @rest_arguments = @argv.dup
      rescue OptionParser::ParseError => err
        raise OptionError, err.message
      end

      attr_reader :rest_arguments, :environment, :log_file_path

      def daemon?
        @daemon
      end

      attr_reader :pid_file_path

    end

  end   # module StreamingLoad

end   # module Bricolage
