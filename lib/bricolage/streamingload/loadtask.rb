module Bricolage

  module StreamingLoad

    class LoadTask

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
          task_id: task_id,
          task_seq: rec['dwh_task_seq'],
          task_class: rec['dwh_task_class'],
          schema: rec['schema_name'],
          table: rec['table_name'],
          initialized: rec['initialized'],
          disabled: rec['disabled']
        )
      end

      def initialize(task_id:, task_seq:, task_class:,
          schema:, table:, initialized:, disabled:)
        @id = task_id
        @seq = task_seq
        @class = task_class
        @schema = schema
        @table = table
        @initialized = initialized
        @disabled = disabled
      end

      attr_reader :id
      attr_reader :seq
      attr_reader :schema
      attr_reader :table

      def initialized?
        @initialized
      end

      def disabled?
        @disabled
      end

    end

  end

end
