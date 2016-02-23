require 'bricolage/streamingload/loadqueue'
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

  end

end
