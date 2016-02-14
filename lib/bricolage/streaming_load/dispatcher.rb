require 'aws-sdk'
require 'json'

module Bricolage

  module StreamingLoad

    class Dispatcher

      def Dispatcher.main
        # FIXME
        ENV['AWS_REGION'] = 'ap-northeast-1'
        require 'pp'
        require 'yaml'

        config = YAML.load(File.read(ARGV[0]))

        recv_queue = ReceiveQueue.new(
          sqs_url: config['recv_queue']['sql_url'],
          visibility_timeout: config['recv_queue']['visibility_timeout']
        )

        load_queue = LoadQueue.new(
          sqs_url: config['load_queue']['sqs_url']
        )

        dispatcher = Dispatcher.new(
          recv_queue: recv_queue,
          load_queue: load_queue,
          url_patterns: url_patterns
        )

        dispatcher.main
      end

      def initialize(recv_queue:, load_queue:, url_patterns:)
        @recv_queue = recv_queue
        @load_queue = load_queue
        @bufs = LoadBufferSet.new(load_queue)
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
        @recv_queue.each_event do |e|
          mid = "handle_#{e.event_id}"
          # just ignore unknown event to make app migration easy
          if self.respond_to?(mid, true)
            __send__(mid, e)
          end
        end
      end

      def handle_graceful(e)
        @goto_terminate = true
      end

      def handle_data(e)
        obj = e.loadable_object(@url_patterns)
        buf = @bufs[obj.qualified_name]
        if buf.empty?
          set_flush_timer obj.qualified_name, buf.load_interval
        end
        load_task = buf.put(obj)
        delete_events(load_task) if load_task
      end

      def set_flush_timer(table_name, sec)
        @recv_queue.send_flush_message FlushMessage.new(table_name, sec)
      end

      def handle_flush(e)
        load_task = @bufs[e.table_name].flush
        delete_events(load_task.source_events) if load_task
      end

      def delete_events(events)
        events.each do |e|
          @recv_queue.delete(e)
        end
      end

    end


    require 'json'

    class FlushMessage
      def initialize(table_name, sec)
        @table_name = table_name
        @delay_seconds = sec
      end

      attr_reader :delay_seconds

      def body
        { 'event' => 'flush', 'tableName' => @table_name }.to_json
      end
    end


    class ReceiveQueue

      def initialize(sqs_url:, visibility_timeout: 1800)
        @queue_url = sqs_url
        @visibility_timeout = visibility_timeout
        @sqs = Aws::SQS::Client.new
      end

      def each_event(&block)
        result = receive_messages()
        unless result and result.successful?
          sleep 15
          return
        end
        Event.for_sql_result(result).each(&block)
      end

      def receive_messages
        @sqs.receive_message(
          queue_url: @queue_url,
          attribute_names: ["All"],
          message_attribute_names: ["All"],
          max_number_of_messages: 10,
          visibility_timeout: @visibility_timeout,
          wait_time_seconds: 10    # is max value
        )
      end

      def delete(event)
        # TODO: use batch request
        @sqs.delete_message(
          queue_url: @queue_url,
          receipt_handle: event.receipt_handle
        )
      end

      def send_flush_message(msg)
        @sqs.send_message(
          queue_url: @queue_url,
          message_body: msg.body,
          delay_seconds: msg.delay_seconds
        )
      end

    end


    class Event

      def Event.for_sqs_result(result)
        result.messages.flat_map {|msg|
          records = JSON.parse(msg.body)['Records']
          records.map {|rec| get_concrete_class(msg, rec).for_sqs_record(msg, rec) }
        }
      end

      def Event.get_concrete_class(msg, rec)
        case
        #when '????' then ControlEvent
        #when '????' then FlushEvent
        when rec['eventSource'] == 'aws:s3'
          S3ObjectEvent
        else
          raise "[FATAL] unknown SQS message record: eventSource=#{rec['eventSource']} event=#{rec['eventName']} message_id=#{msg.message_id}"
        end
      end

      def Event.for_sqs_record(msg, rec)
        new(** Event.parse_sqs_record(msg, rec).merge(parse_sqs_record(msg, rec)))
      end

      def Event.parse_sqs_record(msg, rec)
        {
          message_id: msg.message_id,
          receipt_handle: msg.receipt_handle,
          event: rec['eventName']
        }
      end

      def initialize(message_id:, receipt_handle:, event:)
        @message_id = message_id
        @receipt_handle = receipt_handle
        @event = event
      end

      def event_id
        raise "#{self.class}\#event_id must be implemented"
      end

      attr_reader :message_id
      attr_reader :receipt_handle
      attr_reader :event

      def data?
        false
      end

    end


    class FlushEvent < Event

      def FlushEvent.parse_sqs_record(msg, rec)
        {
          table_name: rec['tableName']
        }
      end

      def event_id
        'flush'
      end

      def initialize(message_id:, receipt_handle:, event:, table_name:)
        super message_id, receipt_handle, event
        @table_name = table_name
      end

      attr_reader :table_name

    end


    class S3ObjectEvent < Event

      def S3ObjectEvent.parse_sqs_record(msg, rec)
        {
          region: rec['awsRegion'],
          bucket: rec['s3']['bucket']['name'],
          key: rec['s3']['object']['key']
        }
      end

      def initialize(message_id:, receipt_handle:, event:, region:, bucket:, key:)
        super message_id, receipt_handle, event
        @region = region
        @bucket = bucket
        @key = key
      end

      def event_id
        'data'
      end

      attr_reader :region
      attr_reader :bucket
      attr_reader :key

      def url
        "s3://#{@bucket}/#{@key}"
      end

      # override
      def data?
        true
      end

      def created?
        /\AObjectCreated:/ =~ @event
      end

      def loadable_object(url_patterns)
        LoadableObject.new(self, url_patterns.match(url))
      end

    end


    require 'forwardable'

    class LoadableObject

      extend Forwardable

      def initialize(event, components)
        @event = event
        @components = components
      end

      attr_reader :event

      def_delegator '@event', :url
      def_delegator '@components', :datasource_id
      def_delegator '@components', :schema_name
      def_delegator '@components', :table_name

      def qualified_name
        "#{schema_name}.#{table_name}"
      end

    end


    class LoadBufferSet

      def initialize(load_queue:, data_source:)
        @load_queue = load_queue
        @ds = data_source
        @buffers = {}
      end

      def [](key)
        (@buffers[key] ||= LoadBuffer.new(key, load_queue: @load_queue, data_source: @ds))
      end

    ene


    require 'securerandom'

    class LoadBuffer

      def initialize(qualified_name, load_queue:, data_source:)
        @qualified_name = qualified_name
        @load_queue = load_queue
        @ds = data_source
        clear
      end

      attr_reader :qualified_name

      def clear
        @buffer = []
        @curr_task_id = "#{Time.now.strftime('%Y%m%d%H%M%S')}_#{'%5d' % Process.pid}_#{SecureRandom.uuid}"
      end

      def empty?
        @buffer.empty?
      end

      BUFFER_SIZE_MAX = 1000

      def put(obj)
        @buffer.push obj
        if @buffer.size >= BUFFER_SIZE_MAX
          return flush
        else
          return nil
        end
      end

      def flush
        objects = @buffer.freeze
        return nil if objects.empty?
        task = LoadTask.new(objects)
        @load_queue.put task
        clear
        return task
      end

    end


    require 'json'

    class LoadTask

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
          'event' => 'load',
          'source' => 's3',
          'dwhTaskId' => @task_id,
          'objectCount' => @objects.size.to_s
        }.to_json
      end

    end


    class LoadQueue

      def initialize(sqs_url:)
        @queue_url = sqs_url
        @sqs = Aws::SQS::Client.new
      end

      def put(task)
        @sqs.send_message(
          queue_url: @queue_url,
          message_body: task.serialize
        )
      end

    end

  end   # module StreamingLoad

end   # module Bricolage
