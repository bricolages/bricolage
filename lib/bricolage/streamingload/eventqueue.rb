require 'bricolage/streamingload/event'
require 'json'
require 'time'

module Bricolage

  module StreamingLoad

    class ShutdownMessage
      def delay_seconds
        0
      end

      def body
        { 'eventName' => 'shutdown' }
      end
    end


    class FlushMessage
      def initialize(table_name, sec, head_url)
        @table_name = table_name
        @delay_seconds = sec
        @head_url = head_url
        @time = Time.now.getutc
      end

      attr_reader :delay_seconds

      def body
        { 'eventName' => 'flush', 'eventTime' => @time.iso8601, 'tableName' => @table_name, 'headUrl' => @head_url }
      end
    end


    class EventQueue

      def initialize(sqs:, sqs_url:, visibility_timeout: 1800, logger: logger)
        @sqs = sqs
        @queue_url = sqs_url
        @visibility_timeout = visibility_timeout
        @logger = logger
      end

      def each(&block)
        result = receive_messages()
        unless result and result.successful?
          @logger.error "ReceiveMessage failed: #{result.error.message}"
          sleep 15
          return
        end
        events = Event.for_sqs_result(result)
        events.each(&block)
        events.size
      end

      def receive_messages
        @sqs.receive_message(
          queue_url: @queue_url,
          attribute_names: ["All"],
          message_attribute_names: ["All"],
          max_number_of_messages: 10,   # is max value
          visibility_timeout: @visibility_timeout,
          wait_time_seconds: 10   # is max value
        )
      end

      def delete(event)
        # TODO: use batch request
        @sqs.delete_message(
          queue_url: @queue_url,
          receipt_handle: event.receipt_handle
        )
      end

      def send_message(msg)
        @sqs.send_message(
          queue_url: @queue_url,
          message_body: { 'Records' => [msg.body] }.to_json,
          delay_seconds: msg.delay_seconds
        )
      end

    end

  end

end
