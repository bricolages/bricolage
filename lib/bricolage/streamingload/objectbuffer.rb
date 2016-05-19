require 'bricolage/streamingload/task'
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

      def initialize(task_queue:, data_source:, buffer_size_max: 500, default_load_interval: 600, logger:)
        @task_queue = task_queue
        @ds = data_source
        @buffer_size_max = buffer_size_max
        @default_load_interval = default_load_interval
        @logger = logger
        @buffers = {}
      end

      def put(obj)
        self[obj.qualified_name].put obj
      end

      def empty?
        @buffers.values.all? {|buf| buf.empty?}
      end

      def full?
        @buffers.values.map {|buf| buf.size}.reduce {|sum, n| sum + n} >= @buffer_size_max
      end

      def [](key)
        (@buffers[key] ||= TableObjectBuffer.new(key, logger: @logger))
      end

      def flush
        return nil if empty?
        tasks = @buffers.values.map {|buf| buf.flush}.compact
        write_task_payloads tasks
        tasks.each {|task| @task_queue.put task}
        return tasks
      end

      def flush_if(head_url:)
        return nil if empty?
        if @buffers.values.any? {|buf| buf.head_url == head_url}
          flush
        else
          nil
        end
      end

      def load_interval
        # FIXME: load table property from the parameter table
        @default_load_interval
      end

      private

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

      def initialize(qualified_name, logger:)
        @qualified_name = qualified_name
        @schema, @table = qualified_name.split('.', 2)
        @buffer = nil
        @curr_task_id = nil
        @logger = logger
        clear
      end

      attr_reader :qualified_name, :schema, :table, :curr_task_id

      def empty?
        @buffer.empty?
      end

      def size
        @buffer.size
      end

      def head_url
        return nil if empty?
        @buffer.first.url
      end

      def clear
        @buffer = []
        @curr_task_id = "#{Time.now.strftime('%Y%m%d%H%M%S')}_#{'%05d' % Process.pid}_#{SecureRandom.uuid}"
      end

      def put(obj)
        # FIXME: take AWS region into account (Redshift COPY stmt cannot load data from multiple regions)
        @buffer.push obj
        obj
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
