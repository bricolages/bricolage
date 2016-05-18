require 'bricolage/exception'
require 'bricolage/version'
require 'bricolage/sqsdatasource'
require 'bricolage/streamingload/event'
require 'bricolage/streamingload/objectbuffer'
require 'bricolage/streamingload/urlpatterns'
require 'aws-sdk'
require 'yaml'
require 'optparse'

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

        config = YAML.load(File.read(config_path))
        ctx = Context.for_application('.')
        event_queue = ctx.get_data_source('sqs', config.fetch('event-queue-ds'))
        task_queue = ctx.get_data_source('sqs', config.fetch('task-queue-ds'))

        object_buffer = ObjectBuffer.new(
          task_queue: task_queue,
          data_source: ctx.get_data_source('sql', 'sql'),
          buffer_size_max: 500,
          default_load_interval: 60,
          logger: ctx.logger
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
        dispatcher.event_loop
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
        if @object_buffer.empty?
          set_flush_timer obj.qualified_name, @object_buffer.load_interval, obj.url
        end
        @object_buffer.put(obj)
        if @object_buffer.full?
          load_tasks = @object_buffer.flush
          load_tasks.each {|load_task| delete_events(load_task.source_events)} if load_tasks
        end
      end

      def set_flush_timer(table_name, sec, head_url)
        @event_queue.send_message FlushEvent.create(table_name: table_name, delay_seconds: sec, head_url: head_url)
      end

      def handle_flush(e)
        load_tasks = @object_buffer.flush_if(head_url: e.head_url)
        load_tasks.each {|load_task| delete_events(load_task.source_events)} if load_tasks
        @event_queue.delete_message(e)
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
        @pid_file_path = nil
        @rest_arguments = nil

        @opts = opts = OptionParser.new("Usage: #{$0} CONFIG_PATH")
        opts.on('--task-id=ID', 'Execute oneshot load task (implicitly disables daemon mode).') {|task_id|
          @task_id = task_id
        }
        opts.on('--daemon', 'Becomes daemon in server mode.') {
          @daemon = true
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

      attr_reader :rest_arguments

      def daemon?
        @daemon
      end

      attr_reader :pid_file_path

    end

  end   # module StreamingLoad

end   # module Bricolage
