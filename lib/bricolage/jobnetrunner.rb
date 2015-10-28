require 'bricolage/application'
require 'bricolage/context'
require 'bricolage/jobflow'
require 'bricolage/taskqueue'
require 'bricolage/job'
require 'bricolage/jobresult'
require 'bricolage/datasource'
require 'bricolage/variables'
require 'bricolage/eventhandlers'
require 'bricolage/logger'
require 'bricolage/exception'
require 'bricolage/version'
require 'pathname'
require 'optparse'

module Bricolage

  class JobNetRunner
    def JobNetRunner.main
      new.main
    end

    def initialize
      Signal.trap('PIPE', 'IGNORE')
      @hooks = ::Bricolage
      @flow_id = nil
      @flow_start_time = Time.now
      @log_path = nil
    end

    EXIT_SUCCESS = JobResult::EXIT_SUCCESS
    EXIT_FAILURE = JobResult::EXIT_FAILURE
    EXIT_ERROR = JobResult::EXIT_ERROR

    def main
      opts = Options.new(self)
      @hooks.run_before_option_parsing_hooks(opts)
      opts.parse ARGV
      @ctx = Context.for_application(nil, opts.jobnet_file, environment: opts.environment, global_variables: opts.global_variables)
      @flow_id = "#{opts.jobnet_file.dirname.basename}/#{opts.jobnet_file.basename('.jobnet')}"
      @log_path = opts.log_path
      flow = RootJobFlow.load(@ctx, opts.jobnet_file)
      queue = get_queue(opts)
      if queue.locked?
        raise ParameterError, "Job queue is still locked. If you are sure to restart jobnet, #{queue.unlock_help}"
      end
      unless queue.queued?
        enqueue_jobs flow, queue
        logger.info "jobs are queued." if opts.queue_exist?
      end
      if opts.list_jobs?
        list_jobs queue
        exit EXIT_SUCCESS
      end
      check_jobs queue
      if opts.check_only?
        puts "OK"
        exit EXIT_SUCCESS
      end
      run_queue queue
      exit EXIT_SUCCESS
    rescue OptionError => ex
      raise if $DEBUG
      usage_exit ex.message, opts.help
    rescue ApplicationError => ex
      raise if $DEBUG
      error_exit ex.message
    end

    def logger
      @ctx.logger
    end

    def get_queue(opts)
      if opts.queue_path
        FileTaskQueue.restore_if_exist(opts.queue_path)
      else
        TaskQueue.new
      end
    end

    def enqueue_jobs(flow, queue)
      seq = 1
      flow.sequential_jobs.each do |ref|
        queue.enq JobTask.new(ref)
        seq += 1
      end
      queue.save
    end

    def list_jobs(queue)
      queue.each do |task|
        puts task.job
      end
    end

    def check_jobs(queue)
      queue.each do |task|
        Job.load_ref(task.job, @ctx).compile
      end
    end

    def run_queue(queue)
      @hooks.run_before_all_jobs_hooks(BeforeAllJobsEvent.new(@flow_id, queue))
      queue.consume_each do |task|
        result = execute_job(task.job, queue)
        unless result.success?
          logger.elapsed_time 'jobnet total: ', (Time.now - @flow_start_time)
          logger.error "[job #{task.job}] #{result.message}"
          @hooks.run_after_all_jobs_hooks(AfterAllJobsEvent.new(false, queue))
          exit result.status
        end
      end
      @hooks.run_after_all_jobs_hooks(AfterAllJobsEvent.new(true, queue))
      logger.elapsed_time 'jobnet total: ', (Time.now - @flow_start_time)
      logger.info "status all green"
    end

    def execute_job(ref, queue)
      logger.debug "job #{ref}"
      job = Job.load_ref(ref, @ctx)
      job.compile
      @hooks.run_before_job_hooks(BeforeJobEvent.new(ref))
      result = job.execute_in_process(make_log_path(ref))
      @hooks.run_after_job_hooks(AfterJobEvent.new(result))
      result
    rescue Exception => ex
      logger.exception ex
      logger.error "unexpected error: #{ref} (#{ex.class}: #{ex.message})"
      JobResult.error(ex)
    end

    def make_log_path(job_ref)
      return nil unless @log_path
      start_time = Time.now
      @log_path.gsub(/%\{\w+\}/) {|var|
        case var
        when '%{flow_start_date}' then @flow_start_time.strftime('%Y%m%d')
        when '%{flow_start_time}' then @flow_start_time.strftime('%Y%m%d_%H%M%S%L')
        when '%{job_start_date}' then start_time.strftime('%Y%m%d')
        when '%{job_start_time}' then start_time.strftime('%Y%m%d_%H%M%S%L')
        when '%{flow}', '%{flow_id}' then @flow_id.gsub('/', '::')
        when '%{subsystem}' then job_ref.subsystem
        when '%{job}', '%{job_id}' then job_ref.name
        else
          raise ParameterError, "bad log path variable: #{var}"
        end
      }
    end

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
        @log_path = nil
        @queue_path = nil
        @check_only = false
        @list_jobs = false
        @global_variables = Variables.new
        @parser = OptionParser.new
        define_options @parser
      end

      attr_reader :environment
      attr_reader :jobnet_file
      attr_reader :log_path
      attr_reader :queue_path

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
        parser.on('--log-path=PATH', 'Log file path template.') {|path|
          @log_path = path
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
