require 'bricolage/sqsdatasource'

module Bricolage

  module StreamingLoad

    class Event < SQSMessage

      def Event.get_concrete_class(msg, rec)
        case
        when rec['eventName'] == 'shutdown' then ShutdownEvent
        when rec['eventName'] == 'processflush' then ProcessFlushEvent
        when rec['eventName'] == 'flush' then FlushEvent
        when rec['eventSource'] == 'aws:s3'
          S3ObjectEvent
        else
          raise "[FATAL] unknown SQS message record: eventSource=#{rec['eventSource']} event=#{rec['eventName']} message_id=#{msg.message_id}"
        end
      end

      def message_type
        raise "#{self.class}\#message_type must be implemented"
      end

      def data?
        false
      end

    end


    class ShutdownEvent < Event

      def ShutdownEvent.create
        super name: 'shutdown'
      end

      def ShutdownEvent.parse_sqs_record(msg, rec)
        {}
      end

      alias message_type name

      def init_message
      end

    end


    class FlushEvent < Event

      def FlushEvent.create(delay_seconds:, table_name:)
        super name: 'flush', delay_seconds: delay_seconds, table_name: table_name
      end

      def FlushEvent.parse_sqs_record(msg, rec)
        {
          table_name: rec['tableName']
        }
      end

      alias message_type name

      def init_message(table_name:)
        @table_name = table_name
      end

      attr_reader :table_name

      def body
        obj = super
        obj['tableName'] = @table_name
        obj
      end

    end

    class ProcessFlushEvent < Event

      def ProcessFlushEvent.create(delay_seconds:)
        super name: 'processflush', delay_seconds: delay_seconds
      end

      alias message_type name

      def init_message(dummy)
      end
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

      def message_type
        'data'
      end

      def init_message(region:, bucket:, key:, size:)
        @region = region
        @bucket = bucket
        @key = key
        @size = size
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
