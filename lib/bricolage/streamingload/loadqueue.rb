require 'json'

module Bricolage

  module StreamingLoad

    class LoadTask

      include Enumerable

      def initialize(task_id:, objects:)
        @task_id = task_id
        @objects = objects
      end

      attr_reader :task_id
      attr_reader :objects

      def source_events
        @objects.map(&:event)
      end

      def serialize
        {
          'eventName' => 'load',
          'eventSource' => 'bricolage:system',
          'dwhTaskId' => @task_id,
          'objectCount' => @objects.size,
          'totalObjectBytes' => @objects.inject(0) {|sz, obj| sz + obj.size }
        }.to_json
      end

      def each(&block)
        @objects.each(&block)
      end

    end


    class LoadQueue

      def initialize(sqs:, sqs_url:)
        @sqs = sqs
        @queue_url = sqs_url
      end

      def put(task)
        @sqs.send_message(
          queue_url: @queue_url,
          message_body: task.serialize,
          delay_seconds: 0
        )
      end

    end

  end   # module StreamingLoad

end   # module Bricolage
