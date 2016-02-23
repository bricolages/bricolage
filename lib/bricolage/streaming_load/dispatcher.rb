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
          data_source: nil,   # FIXME: set ds
          buffer_size_max: 5
        )

        url_patterns = URLPatterns.for_config(config['url_patterns'])

        dispatcher = Dispatcher.new(
          event_queue: event_queue,
          load_buffer: load_buffer,
          url_patterns: url_patterns
        )

        #dispatcher.main
        dispatcher.event_loop
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
          set_flush_timer obj.qualified_name, buf.load_interval
        end
        buf.put(obj)
        if buf.full?
          load_task = buf.flush
          delete_events(load_task.source_events) if load_task
        end
      end

      def set_flush_timer(table_name, sec)
        @event_queue.send_flush_message FlushMessage.new(table_name, sec)
      end

      def handle_flush(e)
        load_task = @bufs[e.table_name].flush
        delete_events(load_task.source_events) if load_task
        @event_queue.delete(e)
      end

      def delete_events(events)
        events.each do |e|
          @event_queue.delete(e)
        end
      end

    end


    class URLPatterns

      def URLPatterns.for_config(configs)
        new(configs.map {|c|
          Pattern.new(url: c.fetch('url'), schema: c.fetch('schema'), table: c.fetch('table'))
        })
      end

      def initialize(patterns)
        @patterns = patterns
      end

      def match(url)
        @patterns.each do |pat|
          components = pat.match(url)
          return components if components
        end
        raise URLPatternNotMatched, "no URL pattern matches the object url: #{url.inspect}"
      end

      class Pattern
        def initialize(url:, schema:, table:)
          @url_pattern = /\A#{url}\z/
          @schema = schema
          @table = table
        end

        attr_reader :url_pattern
        attr_reader :schema
        attr_reader :table

        def match(url)
          m = @url_pattern.match(url) or return nil
          Components.new(get_component(m, @schema), get_component(m, @table))
        end

        def get_component(m, label)
          if /\A%/ =~ label
            m[label[1..-1]]
          else
            label
          end
        end
      end

      Components = Struct.new(:schema_name, :table_name)

    end


    class URLPatternNotMatched < StandardError; end


    require 'json'

    class FlushMessage
      def initialize(table_name, sec)
        @table_name = table_name
        @delay_seconds = sec
      end

      attr_reader :delay_seconds

      def body
        { 'eventName' => 'flush', 'tableName' => @table_name }
      end
    end


    class EventQueue

      def initialize(sqs:, sqs_url:, visibility_timeout: 1800)
        @sqs = sqs
        @queue_url = sqs_url
        @visibility_timeout = visibility_timeout
      end

      def each(&block)
        result = receive_messages()
        unless result and result.successful?
          sleep 15
          return
        end
        events = Event.for_sqs_result(result)
        events.each(&block)
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

      def send_flush_message(msg)
        @sqs.send_message(
          queue_url: @queue_url,
          message_body: { 'Records' => [msg.body] }.to_json,
          delay_seconds: msg.delay_seconds
        )
      end

    end


    class Event

      def Event.for_sqs_result(result)
        result.messages.flat_map {|msg|
          body = JSON.parse(msg.body)
          records = body['Records'] or next []
          records.map {|rec| get_concrete_class(msg, rec).for_sqs_record(msg, rec) }
        }
      end

      def Event.get_concrete_class(msg, rec)
        case
        when rec['eventName'] == 'shutdown' then ShutdownEvent
        when rec['eventName'] == 'flush' then FlushEvent
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
          name: rec['eventName']
        }
      end

      def initialize(message_id:, receipt_handle:, name:)
        @message_id = message_id
        @receipt_handle = receipt_handle
        @name = name
      end

      def event_id
        raise "#{self.class}\#event_id must be implemented"
      end

      attr_reader :message_id
      attr_reader :receipt_handle
      attr_reader :name

      def data?
        false
      end

    end


    class ShutdownEvent < Event

      def ShutdownEvent.parse_sqs_record(msg, rec)
        {}
      end

      def event_id
        'shutdown'
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

      def initialize(message_id:, receipt_handle:, name:, table_name:)
        super message_id: message_id, receipt_handle: receipt_handle, name: name
        @table_name = table_name
      end

      attr_reader :table_name

    end


    class S3ObjectEvent < Event

      def S3ObjectEvent.parse_sqs_record(msg, rec)
        {
          region: rec['awsRegion'],
          bucket: rec['s3']['bucket']['name'],
          key: rec['s3']['object']['key'],
          size: rec['s3']['object']['size']
        }
      end

      def initialize(message_id:, receipt_handle:, name:, region:, bucket:, key:, size:)
        super message_id: message_id, receipt_handle: receipt_handle, name: name
        @region = region
        @bucket = bucket
        @key = key
        @size = size
      end

      def event_id
        'data'
      end

      attr_reader :region
      attr_reader :bucket
      attr_reader :key
      attr_reader :size

      def url
        "s3://#{@bucket}/#{@key}"
      end

      # override
      def data?
        true
      end

      def created?
        /\AObjectCreated:/ =~ @name
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
      def_delegator '@event', :size
      def_delegator '@components', :schema_name
      def_delegator '@components', :table_name

      def qualified_name
        "#{schema_name}.#{table_name}"
      end

    end


    class LoadBufferSet

      def initialize(load_queue:, data_source:, buffer_size_max: 500)
        @load_queue = load_queue
        @ds = data_source
        @buffer_size_max = buffer_size_max
        @buffers = {}
      end

      def [](key)
        (@buffers[key] ||= LoadBuffer.new(key, load_queue: @load_queue, data_source: @ds, buffer_size_max: @buffer_size_max))
      end

    end


    require 'securerandom'

    class LoadBuffer

      def initialize(qualified_name, load_queue:, data_source:, buffer_size_max: 500)
        @qualified_name = qualified_name
        @load_queue = load_queue
        @ds = data_source
        @buffer_size_max = buffer_size_max
        @buffer = nil
        @curr_task_id = nil
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

      def full?
        @buffer.size >= @buffer_size_max
      end

      def put(obj)
        # FIXME: take AWS region into account (Redshift COPY stmt cannot load data from multiple regions)
        @buffer.push obj
        obj
      end

      def flush
        objects = @buffer
        return nil if objects.empty?
        objects.freeze
        task = LoadTask.new(task_id: @curr_task_id, objects: objects)
        @load_queue.put task
        clear
        return task
      end

      def load_interval
        # FIXME: load table property from the parameter table
        600
      end

    end


    require 'json'

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


    class SQSClientWrapper
      def initialize(sqs)
        @sqs = sqs
      end

      def receive_message(**args)
        $stderr.puts "receive_message(#{args.inspect})"
        @sqs.receive_message(**args)
      end

      def send_message(**args)
        $stderr.puts "send_message(#{args.inspect})"
        @sqs.send_message(**args)
      end

      def delete_message(**args)
        $stderr.puts "delete_message(#{args.inspect})"
        @sqs.delete_message(**args)
      end
    end


    class DummySQSClient
      def initialize(queue = [])
        @queue = queue
      end

      def receive_message(**args)
        msg_recs = @queue.shift or return EMPTY_RESULT
        msgs = msg_recs.map {|recs| Message.new({'Records' => recs}.to_json) }
        Result.new(true, msgs)
      end

      def send_message(**args)
        SUCCESS_RESULT
      end

      def delete_message(**args)
        SUCCESS_RESULT
      end

      class Result
        def initialize(successful, messages = nil)
          @successful = successful
          @messages = messages
        end

        def successful?
          @successful
        end

        attr_reader :messages
      end

      SUCCESS_RESULT = Result.new(true)
      EMPTY_RESULT = Result.new(true, [])

      class Message
        def initialize(body)
          @body = body
        end

        attr_reader :body
      end
    end

  end   # module StreamingLoad

end   # module Bricolage
