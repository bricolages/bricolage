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

      def initialize(task_queue:, data_source:, default_buffer_size_limit: 500, default_load_interval: 600, ctx:)
        @task_queue = task_queue
        @ds = data_source
        @default_buffer_size_limit = default_buffer_size_limit
        @default_load_interval = default_load_interval
        @ctx = ctx
        @logger = ctx.logger
        @buffers = {}
      end

      def all_empty?
        @buffers.values.all? {|buf| buf.empty?}
      end

      def any_empty?
        @buffers.values.any? {|buf| buf.empty?}
      end

      # Retrun distinct load_interval(s) of empty buffers
      def empty_buffers_intervals
        @buffers.values.group_by {|buf| buf.load_interval}.select {|i,buf| buf.all? {|b| b.empty?}}.keys
      end

      def any_full?
        @buffers.values.any? {|buf| buf.full?}
      end

      def [](key)
        (@buffers[key] ||= new_table_object_buffer(key))
      end

      def flush(interval:)
        return nil if all_empty?
        buffers = interval ? buffers_with_interval(interval) : @buffers.values
        flush_buffers(buffers)
      end

      def flush_full
        return nil if !any_full?
        flush_buffers(full_buffers)
      end

      # Flush all buffers which has same load_interval as the buffer include head_url
      #TODO  Add load_interval parameter
      def flush_if(head_url:)
        return nil if all_empty?
        buf = @buffers.values.select {|buf| buf.head_url == head_url}
        if buf.size > 0
          flush(interval: buf[0].load_interval)
        else
          nil
        end
      end

      private

      def flush_buffers(buffers)
        tasks = buffers.map {|buf| buf.flush}.compact
        write_task_payloads tasks
        tasks.each {|task| @task_queue.put task}
        return tasks
      end

      def full_buffers
        @buffers.values.select {|buf| buf.full?}
      end

      def buffers_with_interval(interval)
        @buffers.values.select {|buf| buf.load_interval == interval}
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
        clear
      end

      attr_reader :schema, :table, :buffer_size_limit, :load_interval, :curr_task_id

      def qualified_name
        "#{schema}.#{table}"
      end

      def empty?
        @buffer.empty?
      end

      def full?
        size > @buffer_size_limit
      end

      def size
        @buffer.size
      end

      def head_url
        return nil if empty?
        @buffer.first.url
      end

      def put(obj)
        # FIXME: take AWS region into account (Redshift COPY stmt cannot load data from multiple regions)
        @buffer.push obj
        obj
      end

      def clear
        @buffer = []
        @curr_task_id = "#{Time.now.strftime('%Y%m%d%H%M%S')}_#{'%05d' % Process.pid}_#{SecureRandom.uuid}"
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
