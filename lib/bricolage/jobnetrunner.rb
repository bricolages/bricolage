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
require 'bricolage/loglocatorbuilder'
require 'bricolage/logger'
require 'bricolage/exception'
require 'bricolage/version'
require 'fileutils'
require 'pathname'
require 'optparse'
require 'socket'
require 'net/http'
require 'json'

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
    end

    EXIT_SUCCESS = JobResult::EXIT_SUCCESS
    EXIT_FAILURE = JobResult::EXIT_FAILURE
    EXIT_ERROR = JobResult::EXIT_ERROR

    def main
      opts = Options.new(self)
      @hooks.run_before_option_parsing_hooks(opts)
      opts.parse!(ARGV)

      @ctx = Context.for_application(job_path: opts.jobnet_files.first, environment: opts.environment, option_variables: opts.option_variables)
      opts.merge_saved_options(@ctx.load_system_options)

      jobnet = RootJobNet.load_auto(@ctx, opts.jobnet_files)
      @jobnet_id = jobnet.id

      if opts.dump_options?
        puts "jobnet-id=#{@jobnet_id}"
        puts "jobnet-file=#{opts.jobnet_files.first}"
        opts.option_pairs.each do |key, value|
          puts "#{key}=#{value.inspect}"
        end
        exit EXIT_SUCCESS
      end

      if opts.clear_queue?
        clear_queue(opts, jobnet)
        exit EXIT_SUCCESS
      end
      queue = get_queue(opts, jobnet)
      if queue.locked?
        raise ParameterError, "Job queue is still locked. If you are sure to restart jobnet, #{queue.unlock_help}"
      end
      unless queue.queued?
        enqueue_jobs jobnet, queue
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

      @log_locator_builder = LogLocatorBuilder.for_options(@ctx, opts.log_path_format, opts.log_s3_ds, opts.log_s3_key_format)
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

    def clear_queue(opts, jobnet)
      if opts.db_name
        datasource = @ctx.get_data_source('psql', opts.db_name)
        DatabaseTaskQueue.clear_queue(datasource, jobnet)
      elsif path = get_queue_file_path(opts)
        FileUtils.rm_f path
      end
    end

    def get_queue(opts, jobnet)
      if opts.db_name
        datasource = @ctx.get_data_source('psql', opts.db_name)
        executor_id = get_executor_id(opts.executor_type)
        logger.info "DB connect: #{opts.db_name}"
        DatabaseTaskQueue.restore_if_exist(datasource, jobnet, executor_id)
      elsif path = get_queue_file_path(opts)
        logger.info "queue path: #{path}"
        FileTaskQueue.restore_if_exist(path)
      else
        MemoryTaskQueue.new
      end
    end

    def get_executor_id(executor_type)
      # executor_id is 'TaskID:PID' or 'Hostname:PID'
      if executor_type == 'ecs'
        uri = URI.parse("#{ENV['ECS_CONTAINER_METADATA_URI']}/task")
        response = Net::HTTP.get_response(uri)
        task_id = JSON.parse(response.body)['TaskARN'].split('/').last
        "#{task_id}:#{$$}"
      else
        hostname = Socket.gethostname
        "#{hostname}:#{$$}"
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
        queue.enqueue JobTask.new(ref)
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
      result = nil
      task_job = nil
      @hooks.run_before_all_jobs_hooks(BeforeAllJobsEvent.new(@jobnet_id, queue))
      queue.consume_each do |task|
        task_job = task.job
        result = execute_job(task_job, queue)
      end
      @hooks.run_after_all_jobs_hooks(AfterAllJobsEvent.new(result.success?, queue))
      logger.elapsed_time 'jobnet total: ', (Time.now - @jobnet_start_time)

      if result.success?
        logger.info "status all green"
      else
        logger.error "[job #{task_job}] #{result.message}"
        exit result.status
      end
    end

    def execute_job(ref, queue)
      logger.debug "job #{ref}"
      job_start_time = Time.now
      job = Job.load_ref(ref, @ctx)
      job.compile
      @hooks.run_before_job_hooks(BeforeJobEvent.new(ref))
      result = job.execute_in_process(log_locator: make_log_locator(ref, job_start_time))
      @hooks.run_after_job_hooks(AfterJobEvent.new(result))
      result
    rescue Exception => ex
      logger.exception ex
      logger.error "unexpected error: #{ref} (#{ex.class}: #{ex.message})"
      JobResult.error(ex)
    end

    def make_log_locator(ref, job_start_time)
      @log_locator_builder.build(
        job_ref: ref,
        jobnet_id: @jobnet_id,
        job_start_time: job_start_time,
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

    class Options < CommonApplicationOptions
      def initialize(app)
        @app = app
        @environment = nil
        @option_variables = Variables.new
        @jobnet_files = nil

        @dump_options = false
        @check_only = false
        @list_jobs = false
        @clear_queue = false

        init_options
      end

      def opts_default
        super.merge({
          'local-state-dir' => OptionValue.new('default value', '/tmp/bricolage'),
          'enable-queue' => OptionValue.new('default value', false),
          'queue-path' => OptionValue.new('default value', nil),
          'db-name' => OptionValue.new('default value', nil),
          'ecs-executor' => OptionValue.new('default value', false)
        })
      end
      private :opts_default

      def opts_env
        env = super
        if ENV['BRICOLAGE_ENABLE_QUEUE']
          env['enable-queue'] = OptionValue.new('env BRICOLAGE_ENABLE_QUEUE', true)
        end
        if ENV['BRICOLAGE_DISABLE_QUEUE']
          env['enable-queue'] = OptionValue.new('env BRICOLAGE_DISABLE_QUEUE', false)
        end
        if path = ENV['BRICOLAGE_QUEUE_PATH']
          env['queue-path'] = OptionValue.new('env BRICOLAGE_QUEUE_PATH', path)
        end
        env
      end
      private :opts_env

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

        define_common_options

        parser.on('--local-state-dir=PATH', 'Stores local state in this path.') {|path|
          @opts_cmdline['local-state-dir'] = OptionValue.new('--local-state-dir option', path)
        }
        parser.on('-Q', '--enable-queue', 'Enables job queue.') {
          @opts_cmdline['enable-queue'] = OptionValue.new('--enable-queue option', true)
        }
        parser.on('--disable-queue', 'Disables job queue.') {
          @opts_cmdline['enable-queue'] = OptionValue.new('--disable-queue option', false)
        }
        parser.on('--clear-queue', 'Clears job queue and quit.') {
          @clear_queue = true
        }
        parser.on('--queue-path=PATH', 'Enables job queue with this path.') {|path|
          @opts_cmdline['queue-path'] = OptionValue.new('--queue-path option', path)
        }
        parser.on('--db-name=DB_NAME', 'Enables job queue with this database.') {|db_name|
          @opts_cmdline['db-name'] = OptionValue.new('--db-name option', db_name)
        }
        parser.on('--ecs-executor', 'Set executor type as ECS ') {
          @opts_cmdline['ecs-executor'] = OptionValue.new('--ecs-executor option', true)
        }

        parser.on('-c', '--check-only', 'Checks job parameters and quit without executing.') {
          @check_only = true
        }
        parser.on('-l', '--list-jobs', 'Lists target jobs without executing.') {
          @list_jobs = true
        }
        parser.on('-v', '--variable=NAME=VALUE', 'Defines option variable.') {|name_value|
          name, value = name_value.split('=', 2)
          @option_variables[name] = value
        }
        parser.on('--dump-options', 'Shows option parsing result and quit.') {
          @dump_options = true
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

      def parse!(argv)
        @parser.parse!(argv)
        raise OptionError, "missing jobnet file" if argv.empty?
        @jobnet_files = argv.map {|path| Pathname(path) }
        build_common_options!
      rescue OptionParser::ParseError => ex
        raise OptionError, ex.message
      end

      attr_reader :environment

      attr_reader :jobnet_files

      attr_reader :option_variables

      def dump_options?
        @dump_options
      end

      def check_only?
        @check_only
      end

      def list_jobs?
        @list_jobs
      end

      def local_state_dir
        @local_state_dir ||= begin
          opt = @opts['local-state-dir']
          Pathname(opt.value)
        end
      end

      def enable_queue?
        opt = @opts['enable-queue']
        opt.value
      end

      def queue_path
        opt = @opts['queue-path']
        if opt.value
          Pathname(opt.value)
        else
          nil
        end
      end

      def db_name
        opt = @opts['db-name']
        if opt.value
          opt.value
        else
          nil
        end
      end

      def executor_type
        opt = @opts['ecs-executor']
        if opt.value
          'ecs'
        else
          'ec2'
        end
      end

      def clear_queue?
        @clear_queue
      end

      def option_pairs
        common_options.merge({
          'environment' => OptionValue.new(nil, @environment)
        })
      end
    end
  end

end
