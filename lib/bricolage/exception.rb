module Bricolage

  # Common super class of handleable Bricolage exceptions
  class ApplicationError < StandardError; end

  # Job failure.
  # This exception may occur in production environment and is temporary.
  # e.g. Source data error, SQL error
  class JobFailure < ApplicationError; end

  class JobFailureByException < JobFailure
    def JobFailureByException.wrap(ex)
      new(ex.message, ex)
    end

    def initialize(msg, orig = nil)
      super msg
      @original = orig
    end

    attr_reader :original
  end

  # Various SQL exception, except connection problem.
  class SQLException < JobFailureByException; end

  # Database connection problems (not established, closed unexpectedly, invalid state)
  class ConnectionError < JobFailureByException; end

  # Aquiring lock takes too long (e.g. VACUUM lock)
  class LockTimeout < JobFailure; end

  # S3 related exceptions
  class S3Exception < JobFailureByException; end

  # SNS related exceptions
  class SNSException < JobFailureByException; end

  # Job error.
  # This exception should NOT be thrown in production environment.
  # You must fix source code or configuration not to be get this exception.
  class JobError < ApplicationError; end

  # Command-line option errors (should NOT be thrown in production environment)
  class OptionError < JobError; end

  # User parameter errors (should NOT be thrown in production environment)
  class ParameterError < JobError; end

  # Jobnet aborted by any failure or error
  class JobNetSessionAborted < ApplicationError
    def initialize(jobnet, result)
      super "jobnet #{jobnet.id} aborted: cause=#{result.job.ref}"
      @jobnet = jobnet
      @result = result
    end

    attr_reader :jobnet
    attr_reader :result
  end

  # Bad code in bricolage core or job classes.
  # This exception should NOT be thrown in ANY user environment.
  class FatalError < Exception; end

end
