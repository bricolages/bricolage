require 'bricolage/application'
require 'bricolage/context'
require 'bricolage/datasource'
require 'bricolage/jobnet'
require 'bricolage/jobnetsession'
require 'bricolage/jobnetsessionstore'
require 'bricolage/job'
require 'bricolage/jobresult'
require 'bricolage/variables'
require 'bricolage/eventhandlers'
require 'bricolage/logfilepath'
require 'bricolage/logger'
require 'bricolage/exception'
require 'bricolage/version'
require 'pathname'
require 'optparse'

module Bricolage

  class JobNetRunner
    def JobNetRunner.main
      Application.install_signal_handlers
      new.main
    end

    def initialize
      @hooks = ::Bricolage
      @jobnet_id = nil
      @jobnet_start_time = Time.now
      @job_start_time = nil
      @log_path = nil
    end

    EXIT_SUCCESS = JobResult::EXIT_SUCCESS
    EXIT_FAILURE = JobResult::EXIT_FAILURE
    EXIT_ERROR = JobResult::EXIT_ERROR

    def main
      opts = Options.new(self)
      @hooks.run_before_option_parsing_hooks(opts)
      opts.parse ARGV
      @jobnet_id = "#{opts.jobnet_file.dirname.basename}/#{opts.jobnet_file.basename('.jobnet')}"
      store = JobNetSessionStore.for_options(opts, @hooks)
      if session = ss.restore
        @ctx = ss.context
        session.check_lock!
        @ctx.logger.info "jobnet session restored"
      else
        @ctx = Context.for_application(nil, opts.jobnet_file, environment: opts.environment, global_variables: opts.global_variables)
        jobnet = RootJobNet.load(@ctx, opts.jobnet_file)
        session = ss.new_session(jobnet)
        @ctx.logger.info "new jobnet session created"
      end
      if opts.list_jobs?
        list_jobs session.jobs
        exit EXIT_SUCCESS
      end
      check_jobs session.jobs
      if opts.check_only?
        puts "OK"
        exit EXIT_SUCCESS
      end
      session.execute(opts.log_path)
      exit EXIT_SUCCESS
    rescue OptionError => ex
      raise if $DEBUG
      usage_exit ex.message, opts.help
    rescue ApplicationError => ex
      raise if $DEBUG
      error_exit ex.message
    end

    def list_jobs(jobs)
      jobs.each do |job|
        puts job
      end
    end

    def check_jobs(jobs)
      jobs.each do |job|
        Job.load_ref(job, @ctx).compile
      end
    end

    public :puts
    public :exit

    def usage_exit(msg, usage)
      print_error msg
      $stderr.puts usage
      exit 1
    end

    def error_exit(msg)
      print_error msg
      exit 1
    end

    def print_error(msg)
      $stderr.puts "#{program_name}: error: #{msg}"
    end

    def program_name
      File.basename($PROGRAM_NAME, '.*')
    end

    class Options
      def initialize(app)
        @app = app
        @environment = nil
        @jobnet_files = nil
        @log_path = LogFilePath.default
        @queue_path = nil
        @check_only = false
        @list_jobs = false
        @global_variables = Variables.new
        @parser = OptionParser.new
        define_options @parser
      end

      attr_reader :environment
      attr_reader :jobnet_file
      attr_reader :queue_path
      attr_reader :log_path

      def queue_exist?
        !!@queue_path
      end

      def check_only?
        @check_only
      end

      def list_jobs?
        @list_jobs
      end

      attr_reader :global_variables

      def help
        @parser.help
      end

      def define_options(parser)
        parser.banner = <<-EndBanner
Synopsis:
  #{@app.program_name} [options] JOB_NET_FILE
Options:
        EndBanner
        parser.on('-e', '--environment=NAME', "Sets execution environment. [default: #{Context::DEFAULT_ENV}]") {|env|
          @environment = env
        }
        parser.on('--log-dir=PATH', 'Log file prefix.') {|path|
          @log_path = LogFilePath.new("#{path}/%{std}.log")
        }
        parser.on('--log-path=PATH', 'Log file path template.') {|path|
          @log_path = LogFilePath.new(path)
        }
        parser.on('--queue=PATH', 'Use job queue.') {|path|
          @queue_path = Pathname(path)
        }
        parser.on('-c', '--check-only', 'Checks job parameters and quit without executing.') {
          @check_only = true
        }
        parser.on('-l', '--list-jobs', 'Lists target jobs without executing.') {
          @list_jobs = true
        }
        parser.on('-v', '--variable=NAME=VALUE', 'Defines global variable.') {|name_value|
          name, value = name_value.split('=', 2)
          @global_variables[name] = value
        }
        parser.on('--help', 'Shows this message and quit.') {
          @app.puts parser.help
          @app.exit 0
        }
        parser.on('--version', 'Shows program version and quit.') {
          @app.puts "#{APPLICATION_NAME} version #{VERSION}"
          @app.exit 0
        }
      end

      def on(*args, &block)
        @parser.on(*args, &block)
      end

      def parse(argv)
        @parser.parse! argv
        raise OptionError, "missing jobnet file" if argv.empty?
        raise OptionError, "too many jobnet file" if argv.size > 1
        @jobnet_file = argv.map {|path| Pathname(path) }.first
      rescue OptionParser::ParseError => ex
        raise OptionError, ex.message
      end
    end
  end

end
