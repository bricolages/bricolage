require 'bricolage/resource'
require 'bricolage/exception'
require 'forwardable'
require 'stringio'

module Bricolage

  class Script
    def initialize
      @tasks = []
      @in_task_block = false
    end

    def task(ds)
      raise FatalError, "nested task is not supported" if @in_task_block
      raise ParameterError, "no data source" unless ds
      @in_task_block = true
      begin
        task = ds.new_task
        yield task
        @tasks.push task
      ensure
        @in_task_block = false
      end
    end

    def bind(ctx, vars)
      raise "[BUG] unresolved variables given" unless vars.resolved?
      @tasks.each do |task|
        task.bind(ctx, vars)
      end
    end

    def source
      buf = StringIO.new
      first = true
      @tasks.each do |task|
        buf.puts unless first; first = false
        buf.puts task.source
      end
      buf.string
    end

    def run
      result = nil
      @tasks.each do |task|
        result = task.run
      end
      result || JobResult.success
    end

    def run_explain
      @tasks.each do |task|
        if task.respond_to?(:run_explain)
          task.run_explain
        else
          puts "-- task #{task.class} does not support explain; show source"
          puts task.source
        end
      end
    end
  end

  class DataSourceTask
    def initialize(ds)
      @ds = ds
      @actions = []
    end

    attr_reader :ds

    def bind(*args)
      @actions.each do |action|
        action.bind(*args)
      end
    end

    def run
      result = nil
      @ds.open_for_batch {
        @actions.each do |action|
          result = action.run
        end
      }
      result
    end

    def source
      buf = StringIO.new
      @actions.each do |action|
        buf.puts action.source
      end
      buf.string
    end

    private

    def add(action)
      action.ds = @ds
      @actions.push action
    end

    def each_action(&block)
      @actions.each(&block)
    end

    class Action
      extend Forwardable

      attr_accessor :ds

      def bind(context, variables)
      end

      # abstract def run
      # abstract def source
    end
  end

end
