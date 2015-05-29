require 'bricolage/jobclass'

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
      script.task(params.generic_ds) {|task|
        task.action('ruby job') {
          ruby_job.run
          nil   # job result
        }
      }
    end

    def initialize(params, *args)
    end

    def run
      raise "bricolage: error: #{self.class}\#run is not overridden"
    end
  end
end
