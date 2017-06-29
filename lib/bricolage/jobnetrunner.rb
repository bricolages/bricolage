require 'bricolage/application'
require 'bricolage/context'
require 'bricolage/jobnet'
require 'bricolage/taskqueue'
require 'bricolage/job'
require 'bricolage/jobresult'
require 'bricolage/datasource'
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
    end

    EXIT_SUCCESS = JobResult::EXIT_SUCCESS
    EXIT_FAILURE = JobResult::EXIT_FAILURE
    EXIT_ERROR = JobResult::EXIT_ERROR

    def main
      opts = Options.new(self)
      @hooks.run_before_option_parsing_hooks(opts)
      opts.parse ARGV
      @ctx = Context.for_application(job_path: opts.jobnet_file, environment: opts.environment, global_variables: opts.global_variables)
      @jobnet_id = "#{opts.jobnet_file.dirname.basename}/#{opts.jobnet_file.basename('.jobnet')}"
      jobnet =
        if opts.jobnet_file.extname == '.job'
          RootJobNet.load_single_job(@ctx, opts.jobnet_file)
        else
          RootJobNet.load(@ctx, opts.jobnet_file)
        end
      queue = get_queue(opts)
      if queue.locked?
        raise ParameterError, "Job queue is still locked. If you are sure to restart jobnet, #{queue.unlock_help}"
      end
      unless queue.queued?
        enqueue_jobs jobnet, queue
        logger.info "jobs are queued."
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
      run_queue queue, opts.log_path_format
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
      if path = get_queue_file_path(opts)
        logger.info "queue path: #{path}"
        FileTaskQueue.restore_if_exist(path)
      else
        TaskQueue.new
      end
    end

    def get_queue_file_path(opts)
      if opts.queue_path
        opts.queue_path
      elsif opts.enable_queue?
        opts.local_state_dir + 'queue' + "#{app_name}.#{@jobnet_id.tr('/', '.')}"
      else
        nil
      end
    end

    def app_name
      path = @ctx.home_path.realpath
      while /\A(?:\d+|current|releases)\z/ =~ path.basename.to_s   # is Capistrano dirs
        path = path.dirname
      end
      path.basename.to_s
    end

    def enqueue_jobs(jobnet, queue)
      seq = 1
      jobnet.sequential_jobs.each do |ref|
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

    def run_queue(queue, log_path_format)
      @hooks.run_before_all_jobs_hooks(BeforeAllJobsEvent.new(@jobnet_id, queue))
      queue.consume_each do |task|
        result = execute_job(task.job, queue, log_path_format)
        unless result.success?
          logger.elapsed_time 'jobnet total: ', (Time.now - @jobnet_start_time)
          logger.error "[job #{task.job}] #{result.message}"
          @hooks.run_after_all_jobs_hooks(AfterAllJobsEvent.new(false, queue))
          exit result.status
        end
      end
      @hooks.run_after_all_jobs_hooks(AfterAllJobsEvent.new(true, queue))
      logger.elapsed_time 'jobnet total: ', (Time.now - @jobnet_start_time)
      logger.info "status all green"
    end

    def execute_job(ref, queue, log_path_format)
      logger.debug "job #{ref}"
      @job_start_time = Time.now
      job = Job.load_ref(ref, @ctx)
      job.compile
      @hooks.run_before_job_hooks(BeforeJobEvent.new(ref))
      log_path = log_path_format ? build_log_path(log_path_format, ref) : nil
      result = job.execute_in_process(log_path: log_path)
      @hooks.run_after_job_hooks(AfterJobEvent.new(result))
      result
    rescue Exception => ex
      logger.exception ex
      logger.error "unexpected error: #{ref} (#{ex.class}: #{ex.message})"
      JobResult.error(ex)
    end

    def build_log_path(fmt, ref)
      fmt.format(
        job_ref: ref,
        jobnet_id: @jobnet_id,
        job_start_time: @job_start_time,
        jobnet_start_time: @jobnet_start_time
      )
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
        @log_path_format = LogFilePath.default
        @local_state_dir = Pathname('/tmp/bricolage')
        if path = ENV['BRICOLAGE_QUEUE_PATH']
          @enable_queue = true
          @queue_path = Pathname(path)
        elsif ENV['BRICOLAGE_ENABLE_QUEUE']
          @enable_queue = true
          @queue_path = nil
        else
          @enable_queue = false
          @queue_path = nil
        end
        @check_only = false
        @list_jobs = false
        @global_variables = Variables.new
        @parser = OptionParser.new
        define_options @parser
      end

      attr_reader :environment
      attr_reader :jobnet_file
      attr_reader :log_path_format

      attr_reader :local_state_dir

      def enable_queue?
        @enable_queue
      end

      attr_reader :queue_path

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
        parser.on('-L', '--log-dir=PATH', 'Log file prefix.') {|path|
          @log_path_format = LogFilePath.new("#{path}/%{std}.log")
        }
        parser.on('--log-path=PATH', 'Log file path template.') {|path|
          @log_path_format = LogFilePath.new(path)
        }
        parser.on('--local-state-dir=PATH', 'Stores local state in this path.') {|path|
          @local_state_dir = Pathname(path)
        }
        parser.on('-Q', '--enable-queue', 'Enables job queue.') {
          @enable_queue = true
        }
        parser.on('--queue-path=PATH', 'Enables job queue with this path.') {|path|
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
