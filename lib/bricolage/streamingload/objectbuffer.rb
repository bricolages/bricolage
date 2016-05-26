require 'bricolage/streamingload/task'
require 'bricolage/streamingload/loaderparams'
require 'bricolage/sqlutils'
require 'json'
require 'securerandom'
require 'forwardable'

module Bricolage

  module StreamingLoad

    class LoadableObject

      extend Forwardable

      def initialize(event, components)
        @event = event
        @components = components
      end

      attr_reader :event

      def_delegator '@event', :url
      def_delegator '@event', :size
      def_delegator '@event', :message_id
      def_delegator '@event', :receipt_handle
      def_delegator '@components', :schema_name
      def_delegator '@components', :table_name

      def qualified_name
        "#{schema_name}.#{table_name}"
      end

      def event_time
        @event.time
      end

    end


    class ObjectBuffer

      include SQLUtils

      def initialize(task_queue:, data_source:, default_buffer_size_limit: 500, default_load_interval: 600, process_flush_interval: 60, context:)
        @task_queue = task_queue
        @ds = data_source
        @default_buffer_size_limit = default_buffer_size_limit
        @default_load_interval = default_load_interval
        @process_flush_interval = process_flush_interval
        @ctx = context
        @logger = context.logger
        @buffers = {}
      end

      attr_reader :process_flush_interval

      def [](key)
        (@buffers[key] ||= new_table_object_buffer(key))
      end

      def process_flush
        tasks = flush_required_buffers.map {|buf| buf.flush }.compact
        return [] if tasks.empty? # Avoid empty transaction
        write_task_payloads tasks
        tasks.each {|task| @task_queue.put task }
        return tasks
      end

      private

      def flush_required_buffers
        @buffers.values.select {|buf| buf.flush_requested? || buf.full? }
      end

      def new_table_object_buffer(key)
        schema, table = key.split('.', 2)
        job = LoaderParams.load_job(@ctx, schema, table)
        job.compile
        buffer_size_limit = job.params['buffer-size-limit'] || @default_buffer_size_limit
        load_interval = job.params['load-interval'] || @default_load_interval
        TableObjectBuffer.new(schema, table, buffer_size_limit, load_interval, logger: @logger)
      end

      def write_task_payloads(tasks)
        @ds.open {|conn|
          conn.transaction {
            tasks.each do |task|
              task.seq = write_task(conn, task)
            end
            tasks.each do |task|
              task.objects.each do |obj|
                write_task_file(conn, task.seq, obj)
              end
            end
          }
        }
      end

      def write_task(conn, task)
        conn.update(<<-EndSQL)
            insert into dwh_tasks
                ( dwh_task_id
                , dwh_task_class
                , schema_name
                , table_name
                , utc_submit_time
                )
            values
                ( #{s task.id}
                , #{s task.name}
                , #{s task.schema}
                , #{s task.table}
                , getdate() :: timestamp
                )
            ;
        EndSQL

        # Get generated sequence
        dwh_task_seq = conn.query_value(<<-EndSQL)
            select
                dwh_task_seq
            from
                dwh_tasks
            where
                dwh_task_id = #{s task.id}
            ;
        EndSQL
        dwh_task_seq
      end

      def write_task_file(conn, dwh_task_seq, obj)
        conn.update(<<-EndSQL)
            insert into dwh_str_load_files_incoming
                ( dwh_task_seq
                , object_url
                , message_id
                , receipt_handle
                , utc_event_time
                )
            values
                ( #{dwh_task_seq}
                , #{s obj.url}
                , #{s obj.message_id}
                , #{s obj.receipt_handle}
                , #{t obj.event_time}
                )
            ;
        EndSQL
      end

    end

    class TableObjectBuffer

      include SQLUtils

      def initialize(schema, table, buffer_size_limit, load_interval, logger:)
        @schema = schema
        @table = table
        @buffer_size_limit = buffer_size_limit
        @load_interval = load_interval
        @buffer = nil
        @curr_task_id = nil
        @logger = logger
        @flush_requested = false
        clear
      end

      attr_reader :schema, :table, :buffer_size_limit, :load_interval, :curr_task_id

      def qualified_name
        "#{@schema}.#{@table}"
      end

      def empty?
        @buffer.empty?
      end

      def full?
        @buffer.size >= @buffer_size_limit
      end

      def put(obj)
        # FIXME: take AWS region into account (Redshift COPY stmt cannot load data from multiple regions)
        @buffer.push obj
        obj
      end

      def request_flush
        @flush_requested = true
      end

      def flush_requested?
        @flush_requested
      end

      def clear
        @buffer = []
        @curr_task_id = "#{Time.now.strftime('%Y%m%d%H%M%S')}_#{'%05d' % Process.pid}_#{SecureRandom.uuid}"
        @flush_requested = false
      end

      def flush
        objects = @buffer
        return nil if objects.empty?
        @logger.debug "flush initiated: #{@qualified_name} task_id=#{@curr_task_id}"
        objects.freeze
        task = LoadTask.create(
          task_id: @curr_task_id,
          schema: @schema,
          table: @table,
          objects: objects
        )
        clear
        return task
      end

    end

  end

end
