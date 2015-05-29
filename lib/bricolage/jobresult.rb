require 'bricolage/exception'

module Bricolage

  class JobResult
    def JobResult.success
      new(:success)
    end

    def JobResult.failure(ex)
      new(:failiure, exception: ex)
    end

    def JobResult.error(ex)
      new(:error, exception: ex)
    end

    def JobResult.for_bool(is_success, msg = nil)
      new((is_success ? :success : :failure), message: msg)
    end

    def JobResult.for_process_status(st, msg = nil)
      new((st.success? ? :success : :failure), process_status: st, message: msg)
    end

    EXIT_SUCCESS = 0
    EXIT_FAILURE = 1    # production time errors; expected / unavoidable job error
    EXIT_ERROR = 2      # development time errors (e.g. bad option, bad parameter, bad configuration)

    def initialize(type, exception: nil, process_status: nil, message: nil)
      @type = type
      @exception = exception
      @process_status = process_status
      @message = message
    end

    def success?
      @type == :success
    end

    attr_reader :exception
    attr_reader :process_status

    def status_string
      @type.to_s.upcase
    end

    def status
      if @process_status
        @process_status.exitstatus
      else
        case @type
        when :success then EXIT_SUCCESS
        when :failure then EXIT_FAILURE
        when :error then EXIT_ERROR
        else EXIT_ERROR
        end
      end
    end

    alias to_i status

    def message
      if @message
        @message
      elsif @exception
        @exception.message
      else
        success? ? 'suceeded' : 'failed'
      end
    end
  end

end
