require 'bricolage/sqsdatasource'
require 'json'

module Bricolage

  module StreamingLoad

    class Task < SQSMessage

      def Task.get_concrete_class(msg, rec)
        case
        when rec['eventName'] == 'streaming_load_v3' then LoadTask
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


    class LoadTask < Task

      def LoadTask.create(task_id:, schema:, table:, objects:)
        super name: 'streamiong_load_v3', task_id: task_id, schema: schema, table: table, objects: objects
      end

      def LoadTask.parse_sqs_record(msg, rec)
        {
          task_id: rec['dwhTaskId'],
          task_seq: rec['dwhTaskSeq'],
          schema: rec['schemaName'],
          table: rec['tableName'],
          object_count: rec['objectCount'],
          total_object_bytes: rec['totalObjectBytes']
        }
      end

      def LoadTask.load(conn, task_id)
        rec = conn.query_row(<<-EndSQL)
          select
              t.dwh_task_seq
              , t.dwh_task_class
              , t.schema_name
              , t.table_name
              , prm.initialized
              , prm.disabled
          from
              dwh_tasks t
              left outer join dwh_str_load_tables prm using (schema_name, table_name)
          where
              t.dwh_task_id = '#{task_id}'
          ;
        EndSQL
        new(
          name: rec['dwh_task_class'],
          time: nil,
          source: nil,
          task_id: task_id,
          task_seq: rec['dwh_task_seq'],
          schema: rec['schema_name'],
          table: rec['table_name'],
          initialized: rec['initialized'],
          disabled: rec['disabled']
        )
      end

      alias message_type name

      def init_message(task_id:, task_seq: nil,
          schema:, table:, objects: nil,
          initialized: nil, disabled: nil,
          object_count: nil, total_object_bytes: nil)
        @id = task_id
        @seq = task_seq
        @schema = schema
        @table = table

        # Effective only on the queue writer process
        @objects = objects

        # Effective only on the queue reader process
        @initialized = initialized
        @disabled = disabled
      end

      attr_reader :id
      attr_accessor :seq

      attr_reader :schema
      attr_reader :table

      #
      # For writer
      #

      attr_reader :objects

      def source_events
        @objects.map(&:event)
      end

      def body
        obj = super
        obj['dwhTaskId'] = @id
        obj['dwhTaskSeq'] = @seq
        obj['schemaName'] = @schema
        obj['tableName'] = @table
        obj['objectCount'] = @objects.size
        obj['totalObjectBytes'] = @objects.inject(0) {|sz, obj| sz + obj.size }
        obj
      end

      #
      # For reader
      #

      def initialized?
        @initialized
      end

      def disabled?
        @disabled
      end

    end

  end   # module StreamingLoad

end   # module Bricolage
