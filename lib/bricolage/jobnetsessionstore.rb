require 'bricolage/context'
require 'bricolage/jobnet'
require 'bricolage/jobnetsession'

module Bricolage

  class JobNetSessionStore
    def JobNetSessionStore.for_options(opts, hooks)
    end

    def initialize(jobnet_path)
      @context = nil
      @session = nil
      @jobnet_path = jobnet_path
    end

    attr_reader :context

    def restore
      session = ....
      if session.locked?
        raise ParameterError, "Job queue is still locked. If you are sure to restart jobnet, #{queue.unlock_help}"
      end
      session
    end

    def new_session
      @context = Context.for_application(nil, opts.jobnet_file, environment: opts.environment, global_variables: opts.global_variables)
      ....
      jobnet = RootJobNet.load(@context, @jobnet_path)
    end
  end

end
