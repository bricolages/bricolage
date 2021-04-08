require 'bricolage/datasource'

module Bricolage
  class GenericDataSource < DataSource
    declare_type 'generic'

    def initialize
    end

    def new_task
      GenericTask.new(self)
    end
  end

  class GenericTask < DataSourceTask
    def action(label = nil, &block)
      raise FatalError, "no block" unless block
      add AnyAction.new(label, block)
    end

    class AnyAction < Action
      def initialize(label, block)
        @label = label
        @block = block
      end

      def source
        @label || @block.to_s
      end

      def run
        @block.call(ds)
      end
    end
  end
end
