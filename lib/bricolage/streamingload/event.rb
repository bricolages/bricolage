require 'bricolage/streamingload/loadbuffer'
require 'json'

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

  end

end
