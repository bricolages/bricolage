require 'bricolage/streamingload/eventqueue'
require 'bricolage/streamingload/loadbuffer'
require 'bricolage/streamingload/loadqueue'
require 'bricolage/streamingload/urlpatterns'
require 'bricolage/streamingload/sqswrapper'
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

        sqs_client = SQSClientWrapper.new(Aws::SQS::Client.new)
        dummy_client = SQSClientWrapper.new(DummySQSClient.new)

        event_queue = EventQueue.new(
          sqs: sqs_client,
          sqs_url: config['event_queue']['sqs_url'],
          visibility_timeout: config['event_queue']['visibility_timeout']
        )

        load_queue = LoadQueue.new(
          sqs: dummy_client,   #sqs_client,
          sqs_url: config['load_queue']['sqs_url']
        )

        load_buffer = LoadBufferSet.new(
          load_queue: load_queue,
          data_source: ctx.get_data_source('sql', 'sql'),
          buffer_size_max: 3
        )

        url_patterns = URLPatterns.for_config(config['url_patterns'])

        dispatcher = Dispatcher.new(
          event_queue: event_queue,
          load_buffer: load_buffer,
          url_patterns: url_patterns
        )

        #dispatcher.main
        #dispatcher.event_loop
        dispatcher.handle_events
      end

      def initialize(event_queue:, load_buffer:, url_patterns:)
        @event_queue = event_queue
        @bufs = load_buffer
        @url_patterns = url_patterns
        @goto_terminate = false
      end

      def main
        #trap_signals
        #daemon
        event_loop
      end

      def trap_signals
        # Allows graceful stop
        Signal.trap(:TERM) {
          @goto_terminate = true
        }
      end

      def event_loop
        until @goto_terminate
          handle_events
        end
      end

      def handle_events
        # FIXME: insert wait?
        @event_queue.each do |e|
          mid = "handle_#{e.event_id}"
          # just ignore unknown event to make app migration easy
          if self.respond_to?(mid, true)
            __send__(mid, e)
          end
        end
      end

      def handle_shutdown(e)
        @goto_terminate = true
        @event_queue.delete(e)
      end

      def handle_data(e)
        unless e.created?
          @event_queue.delete(e)
          return
        end
        obj = e.loadable_object(@url_patterns)
        buf = @bufs[obj.qualified_name]
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
        @event_queue.send_message FlushMessage.new(table_name, sec, head_url)
      end

      def handle_flush(e)
        load_task = @bufs[e.table_name].flush_if(head_url: e.head_url)
        delete_events(load_task.source_events) if load_task
        @event_queue.delete(e)
      end

      def delete_events(events)
        events.each do |e|
          @event_queue.delete(e)
        end
      end

    end

  end

end
