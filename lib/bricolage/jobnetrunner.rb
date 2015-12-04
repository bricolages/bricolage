require 'bricolage/application'
require 'bricolage/context'
require 'bricolage/jobnet'
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
      @jobnet_id = nil
      @jobnet_start_time = Time.now
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
      @jobnet_id = "#{opts.jobnet_file.dirname.basename}/#{opts.jobnet_file.basename('.jobnet')}"
      @log_path = opts.log_path
      jobnet = RootJobNet.load(@ctx, opts.jobnet_file)
      queue = get_queue(opts)
      if queue.locked?
        raise ParameterError, "Job queue is still locked. If you are sure to restart jobnet, #{queue.unlock_help}"
      end
      unless queue.queued?
        enqueue_jobs jobnet, queue
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
      opts.jobnet_deps.wait
      run_queue queue
      opts.trigger_dir.create_file_for_jobnet(opts.jobnet_file) if opts.trigger_dir
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

    def run_queue(queue)
      @hooks.run_before_all_jobs_hooks(BeforeAllJobsEvent.new(@jobnet_id, queue))
      queue.consume_each do |task|
        result = execute_job(task.job, queue)
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
        when '%{jobnet_start_date}' then @jobnet_start_time.strftime('%Y%m%d')
        when '%{jobnet_start_time}' then @jobnet_start_time.strftime('%Y%m%d_%H%M%S%L')
        when '%{job_start_date}' then start_time.strftime('%Y%m%d')
        when '%{job_start_time}' then start_time.strftime('%Y%m%d_%H%M%S%L')
        when '%{jobnet}', '%{net}', '%{jobnet_id}', '%{net_id}', '%{flow}', '%{flow_id}' then @jobnet_id.gsub('/', '::')
        when '%{subsystem}' then job_ref.subsystem
        when '%{job}', '%{job_id}' then job_ref.name
        else
          raise ParameterError, "bad log path variable: #{var}"
        end
      }
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
        @log_path = nil
        @queue_path = nil
        @check_only = false
        @list_jobs = false
        @global_variables = Variables.new
        @jobnet_deps = []
        @trigger_dir = TriggerDir.default
        @parser = OptionParser.new
        define_options @parser
      end

      attr_reader :environment
      attr_reader :jobnet_file
      attr_reader :log_path
      attr_reader :queue_path
      attr_reader :jobnet_deps
      attr_reader :trigger_dir

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
        parser.on('-w', '--depend-on=JOBNET:MAX_WAIT_MIN', 'Waits JOBNET before execution.') {|name_wait|
          jobnet_name, m = name_wait.split(':', 2)
          max_wait_min = m ? [m.to_i, 0].max : 0
          @jobnet_deps.add jobnet_name, (max_wait_min > 0 ? max_wait_min : nil)
        }
        parser.on('--trigger-dir=PATH', "Trigger file directory. [default: #{TriggerDir::ENV_KEY}=#{TriggerDir.default_path}]") {|path|
          @trigger_dir = TriggerDir.new(path)
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

  class JobNetDependencies
    def initialize(trigger_dir)
      @trigger_dir = trigger_dir
      @deps = []
    end

    attr_reader :trigger_dir

    def add(jobnet_name, max_wait_min)
      @deps.push Dep.new(self, jobnet_name, (max_wait_min > 0 ? max_wait_min : nil))
    end

    WAIT_INTERVAL = 10     # 10 seconds
    REPORT_INTERVAL = 300  # 5 minutes

    def wait
      start_time = Time.now
      last_report_time = start_time
      until ready?
        now = Time.now
        if now - last_report_time > REPORT_INTERVAL
          logger.info "waiting jobnets..."
          last_report_time = now
        end
        if d = @deps.detect {|dep| dep.max_wait_min && (now - start_time > dep.max_wait_min * 60) }
          raise JobFailure, "waiting jobnets too long: #{d.name.inspect}"
        end
        sleep WAIT_INTERVAL
      end
    end

    def ready?
      @deps.all(&:ready?)
    end

    class Dep
      def initialize(deps, name, max_wait_min = nil)
        @deps = deps
        @name = name
        @max_wait_min = max_wait_min
        @ready = nil
      end

      attr_reader :name
      attr_reader :max_wait_min

      def ready?
        @ready ||= trigger_file.exist?
      end

      def trigger_file
        @trigger_file ||= TriggerFile.new(@name, prefix: @deps.trigger_dir)
      end
    end
  end

  class TriggerDir
    ENV_KEY = 'BRICOLAGE_TRIGGER_DIR'

    def TriggerDir.default_path
      ENV[ENV_KEY]
    end

    def TriggerDir.default
      new(default_dir)
    end

    def initialize(path)
      @path = path
    end

    attr_reader :path
    alias to_s path

    def create_file_for_jobnet(jobnet_path)
      TriggerFile.for_jobnet_path(jobnet_path, prefix: @path).create
    end
  end

  class TriggerFile
    def TriggerFile.for_jobnet_path(path, prefix: TriggerDir.default_path)
      new(name_for_path(path), prefix: prefix)
    end

    def TriggerFile.name_for_path(path)
      path = Pathname(path)
      subsys = path.dirname.basename
      base = path.basename('.jobnet')
      "#{subsys}/#{base}"
    end

    def TriggerFile.for_name(name, prefix: TriggerDir.default_path)
      new(name, prefix: prefix)
    end

    def initialize(name, prefix: ENV['BRICOLAGE_TRIGGER_DIR'])
      @prefix = prefix
      @name = name
    end

    attr_reader :prefix
    attr_reader :name

    def path
      return nil unless @prefix
      File.join(prefix, @name.tr('/', '.'))
    end

    def exist?
      File.file?(path)
    end

    def create
      return unless @prefix
      FileUtils.mkdir_p @prefix
      File.open(path, 'w') {|f|
        f.puts %Q({"status":"success",\n"jobnet": "#{@name}",\n"finished_time":"#{Time.now}"})
      }
    end
  end

end
