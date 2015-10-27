require 'bricolage/jobclass'
require 'bricolage/datasource'
require 'forwardable'

module Bricolage

  class RubyJobClass
    def RubyJobClass.job_class_id(id)
      wrapper = self
      JobClass.define(id) {
        job_class = self
        wrapper.define_job_class(job_class)
      }
    end

    def RubyJobClass.define_job_class(job_class)
      job_class.parameters {|params| parameters(params) }
      job_class.declarations {|params| declarations(params) }
      job_class.script {|params, script| script(params, script) }
    end

    def RubyJobClass.parameters(params)
    end

    def RubyJobClass.declarations(params)
    end

    def RubyJobClass.script(params, script)
      ruby_job = new(params)
      script.task(params.ruby_ds) {|task|
        task.bind_ruby_job ruby_job
      }
    end

    def initialize(params, *args)
    end

    def bind(ctx, vars)
    end

    def source
      'ruby job'
    end

    def run
      raise "bricolage: error: #{self.class}\#run is not overridden"
    end
  end

  class RubyDataSource < DataSource
    declare_type 'ruby'

    # FIXME: keyword argument placeholder is required
    def initialize(**)
    end

    def new_task
      RubyTask.new(self)
    end
  end

  class RubyTask < DataSourceTask
    def bind_ruby_job(ruby_job)
      add RubyAction.new(ruby_job)
    end

    class RubyAction < Action
      def initialize(ruby_job)
        @ruby_job = ruby_job
      end

      extend Forwardable
      def_delegators '@ruby_job', :source, :bind, :run
    end
  end

end
