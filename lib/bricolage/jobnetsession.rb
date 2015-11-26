require 'bricolage/exception'

module Bricolage

  class JobNetSession
    def initialize(executor, jobnet, hooks:)
      @ex = executor
      @jobnet = jobnet
      @hooks = hooks
      @start_time = Time.now

      # State Management
      @ready = [@jobnet.start_jobnet.start]
      @running = []
      @done = []
    end

    attr_reader :start_time

    def elapsed_time
      Time.now - @start_time
    end

    #
    # State Transition
    #

    def staged_to_ready(ref)
      @ready.push ref
    end

    def ready_to_running(ref)
      @ready.delete ref
      @running.push ref
    end

    def running_to_done(ref)
      @running.delete ref
      @done.push ref
    end

    #
    # Notification
    #

    def logger
      @ex.context.logger
    end

    def notify_start
      @hooks.run_before_all_jobs_hooks(BeforeAllJobsEvent.new(@jobnet.id, self))
    end

    def notify_success
      @hooks.run_after_all_jobs_hooks(AfterAllJobsEvent.new(true, queue))
      logger.elapsed_time 'jobnet total: ', session.elapsed_time
      logger.info "status all green"
    end

    def notify_failure
      @hooks.run_after_all_jobs_hooks(AfterAllJobsEvent.new(false, self))
      logger.elapsed_time 'jobnet total: ', session.elapsed_time
      logger.error "[job #{task.job}] #{result.message}"
    end

    #
    # Job Net Execution
    #

    def execute
      notify_start
      while @ex.wait_for_status_change
        run_jobnet_partial
      end
      notify_success
    end

    def run_jobnet_partial
      while result = @ex.wait_job
        @hooks.run_after_job_hooks(AfterJobEvent.new(result))
        if abort_jobnet?(result)
          notify_error
          @ex.abort
          raise JobNetSessionAborted.new(@jobnet, result)
        end
        @jobnet.ready_jobs(result.job.ref).each do |ready_job|
          staged_to_ready ready_job
        end
        running_to_done fin_job
      end
      while job = startable_job()
        @hooks.run_before_job_hooks(BeforeJobEvent.new(job))
        @ex.start_job(ref)
        ready_to_running job
      end
    end

    def abort_jobnet?(result)
      # should implement ignorable job
      not result.success?
    end

    def startable_job
      return nil if @ready.empty?
      return nil unless @ex.acceptable?
      # FIXME: implement Data-source sensitive job selection
      # e.g. max 3 for Redshift, max 2 for MySQL, max 20 for S3
      @ready.first
    end
  end

  class AbstractJobExecutor
    def initialize(context, log_path:)
      @context = context
      @log_path = log_path
    end

    attr_reader :context

    def acceptable(jobs)
      jobs.first
    end

    def start_job(ref)
      job = Job.load_ref(ref, @ctx)
      job.compile
      _start(job)
    end

    #abstract _start

    #abstract wait_for_status_change

    def wait_job
      begin
        result = _wait()
      rescue Exception => ex
        logger.exception ex
        logger.error "unexpected error: #{ref} (#{ex.class}: #{ex.message})"
        JobResult.error(ex)
      end
      result
    end

    #abstract _wait

    def abort
    end

    private

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

    def redirect_stdouts_to(path)
      FileUtils.mkdir_p File.dirname(path)
      # make readable for retrieve_last_match_from_stderr
      File.open(path, 'w+') {|f|
        $stdout.reopen f
        $stderr.reopen f
      }
    end
  end

  class ParallelJobExecutor < AbstractJobExecutor
    def initialize(context, log_path:, n_max_jobs: 3)
      super context, log_path: log_path
      @n_max_jobs = n_max_jobs
      @n_running = 0
      @last_pid = nil
      @last_status = nil
    end

    attr_accessor :n_max_jobs

    def _start(job)
      log_path = make_log_path(job.ref)
      pid = Process.fork {
        Process.setproctitle "bricolage [#{job.id}]"
        redirect_stdouts_to log_path if log_path
        result = job.execute
        save_result result, log_path
        exit result.status
      }
      @n_running += 1
      pid
    end

    def wait_for_status_change
      @last_pid, @last_status = Process.wait2
      @n_running -= 1
    end

    def _wait
      if @last_status
        st = @last_status
        @last_status = nil
      else
        @last_pid, st = Process.wait2
        @n_running -= 1
      end
      restore_result(st, log_path)
    end

    def abort
      super
      detach_children
    end

    def detach_children
      Process.wait(-1, Process::WNOHANG | Process::WUNTRACED)
    end

    private

    def save_result(result, log_path)
      return if result.success?
      return unless log_path
      begin
        File.open(error_log_path(log_path), 'w') {|f|
          f.puts result.message
        }
      rescue
      end
    end

    def restore_result(st, log_path)
      JobResult.for_process_status(st, restore_message(log_path))
    end

    def restore_message(log_path)
      return nil unless log_path
      msg = read_if_exist(error_log_path(log_path))
      msg ? msg.strip : nil
    ensure
      FileUtils.rm_f error_log_path(log_path) if log_path
    end

    def error_log_path(log_path)
      "#{log_path}.error"
    end

    def read_if_exist(path)
      File.read(path)
    rescue
      nil
    end
  end

end
