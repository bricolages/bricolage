module Bricolage
  class EventHandlers
    def initialize
      @handlers = []
    end

    def add(handler)
      @handlers.push handler
    end

    def run(event)
      @handlers.each do |h|
        begin
          h.call(event)
        rescue => err
          $stderr.puts "hook error: #{err.class}: #{err.message}"
          err.backtrace.each do |line|
            $stderr.puts "\t#{line}"
          end
        end
      end
    end
  end

  %w[
    before_option_parsing
    before_all_jobs
    before_job
    after_job
    after_all_jobs
  ].each do |type|
    handlers = EventHandlers.new
    instance_variable_set "@#{type}".intern, handlers
    cc = (class << self; self; end)
    cc.__send__(:define_method, type.intern) {|&action|
      handlers.add(action)
    }
    cc.__send__(:define_method, "run_#{type}_hooks".intern) {|event|
      handlers.run(event)
    }
  end

  BeforeAllJobsEvent = Struct.new(:flow_id, :queue)
  BeforeJobEvent = Struct.new(:job)
  AfterJobEvent = Struct.new(:result)
  AfterAllJobsEvent = Struct.new(:succeeded, :queue)
end
