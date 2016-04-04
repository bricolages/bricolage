require 'bricolage/sqsdatasource'
require 'bricolage/streamingload/event'
require 'bricolage/streamingload/objectbuffer'
require 'bricolage/streamingload/urlpatterns'
require 'aws-sdk'

module Bricolage

  module StreamingLoad

    class Dispatcher

      def Dispatcher.main
        # FIXME
        ENV['AWS_REGION'] = 'ap-northeast-1'
        require 'pp'
        require 'yaml'
        require 'bricolage'

        config = YAML.load(File.read(ARGV[0]))

        ctx = Context.for_application('.')

        event_queue = ctx.get_data_source('sqs', config.fetch('event-queue-ds'))
        task_queue = ctx.get_data_source('sqs', config.fetch('task-queue-ds'))

        object_buffer = ObjectBuffer.new(
          task_queue: task_queue,
          data_source: ctx.get_data_source('sql', 'sql'),
          buffer_size_max: 10,
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

        #dispatcher.main
        dispatcher.event_loop
      end

      def initialize(event_queue:, object_buffer:, url_patterns:, logger:)
        @event_queue = event_queue
        @object_buffer = object_buffer
        @url_patterns = url_patterns
        @logger = logger
      end

      def main
        #daemon
        event_loop
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
        if buf.empty?
          set_flush_timer obj.qualified_name, buf.load_interval, obj.url
        end
        buf.put(obj)
        if buf.full?
          load_task = buf.flush
          delete_events(load_task.source_events) if load_task
        end
      end

      def set_flush_timer(table_name, sec, head_url)
        @event_queue.send_message FlushEvent.create(table_name: table_name, delay_seconds: sec, head_url: head_url)
      end

      def handle_flush(e)
        load_task = @object_buffer[e.table_name].flush_if(head_url: e.head_url)
        delete_events(load_task.source_events) if load_task
        @event_queue.delete_message(e)
      end

      def delete_events(events)
        events.each do |e|
          @event_queue.delete_message(e)
        end
      end

    end

  end

end
