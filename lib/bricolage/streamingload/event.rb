require 'bricolage/sqsdatasource'

module Bricolage

  module StreamingLoad

    class Event < SQSMessage

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

      def event_id
        raise "#{self.class}\#event_id must be implemented"
      end

      def data?
        false
      end

    end


    class ShutdownEvent < Event

      def ShutdownEvent.create
        super name: 'shutdown', table_name: table_name, head_url: head_url
      end

      def ShutdownEvent.parse_sqs_record(msg, rec)
        {}
      end

      def event_id
        'shutdown'
      end

      def init_message(table_name:, head_url:)
        @table_name = table_name
        @head_url = head_url
      end

      attr_reader :table_name
      attr_reader :head_url

    end


    class FlushEvent < Event

      def FlushEvent.create(table_name:, head_url:, delay_seconds:)
        super name: 'flush', table_name: table_name, head_url: head_url, delay_seconds: delay_seconds
      end

      def FlushEvent.parse_sqs_record(msg, rec)
        {
          table_name: rec['tableName'],
          head_url: rec['headUrl']
        }
      end

      def event_id
        'flush'
      end

      def init_message(table_name:, head_url:)
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

      def event_id
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
