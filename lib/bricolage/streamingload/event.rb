require 'json'
require 'time'

module Bricolage

  module StreamingLoad

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
        time_str = rec['eventTime']
        tm = time_str ? (Time.parse(time_str) rescue nil) : nil
        {
          message_id: msg.message_id,
          receipt_handle: msg.receipt_handle,
          name: rec['eventName'],
          time: tm
        }
      end

      def initialize(message_id:, receipt_handle:, name:, time:)
        @message_id = message_id
        @receipt_handle = receipt_handle
        @name = name
        @time = time
      end

      def event_id
        raise "#{self.class}\#event_id must be implemented"
      end

      attr_reader :message_id
      attr_reader :receipt_handle
      attr_reader :name
      attr_reader :time

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
          table_name: rec['tableName'],
          head_url: rec['headUrl']
        }
      end

      def event_id
        'flush'
      end

      def initialize(message_id:, receipt_handle:, name:, time:, table_name:, head_url:)
        super message_id: message_id, receipt_handle: receipt_handle, name: name, time: time
        @table_name = table_name
        @head_url = head_url
      end

      attr_reader :table_name
      attr_reader :head_url

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

      def initialize(message_id:, receipt_handle:, name:, time:, region:, bucket:, key:, size:)
        super message_id: message_id, receipt_handle: receipt_handle, name: name, time: time
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
        !!(/\AObjectCreated:(?!Copy)/ =~ @name)
      end

      def loadable_object(url_patterns)
        LoadableObject.new(self, url_patterns.match(url))
      end

    end

  end

end
