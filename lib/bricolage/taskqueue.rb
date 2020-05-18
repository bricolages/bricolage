require 'bricolage/jobnet'
require 'bricolage/sqlutils'
require 'bricolage/exception'
require 'bricolage/dao/job'
require 'bricolage/dao/jobnet'
require 'bricolage/dao/jobexecution'
require 'fileutils'
require 'pathname'
require 'pg'

module Bricolage

  class MemoryTaskQueue

    def initialize
      @queue = []
    end

    def empty?
      @queue.empty?
    end

    def size
      @queue.size
    end

    def each(&block)
      @queue.each(&block)
    end

    def consume_each
      while job = @queue.first
        result = yield job
        break unless result.success?
        @queue.shift
      end
    end

    def restore_jobnet(jobnet)
    end

    def enqueue_jobnet(jobnet)
      jobnet.sequential_jobs.each do |job|
        @queue.push job
      end
    end

    def locked?(jobnet)
      false
    end

    def unlock_help(jobnet)
      raise "[BUG] this message must not be shown"
    end

    def cancel_jobnet(jobnet, message)
      @queue.clear
    end

  end


  class FileTaskQueue

    def initialize(path:)
      @path = path
      @queue = []
    end

    def empty?
      @queue.empty?
    end

    def size
      @queue.size
    end

    def restore_jobnet(jobnet)
      return unless File.exist?(@path)
      File.foreach(@path) do |line|
        @queue.push Task.deserialize(line)
      end
    end

    def enqueue_jobnet(jobnet)
      jobnet.sequential_jobs.each do |ref|
        @queue.push Task.new(ref)
      end
    end

    def each
      @queue.each do |task|
        yield task.job
      end
    end

    def consume_each
      lock
      save
      while task = @queue.first
        task_result = yield task.job
        break unless task_result.success?
        @queue.shift
        save
      end
    ensure
      unlock
    end

    private def save
      if empty?
        @path.unlink if @path.exist?
        return
      end
      FileUtils.mkdir_p @path.dirname
      tmpname = "#{@path}.tmp.#{Process.pid}"
      begin
        File.open(tmpname, 'w') {|f|
          @queue.each do |task|
            f.puts task.serialize
          end
        }
        File.rename tmpname, @path
      ensure
        FileUtils.rm_f tmpname
      end
    end

    def locked?(jobnet)
      lock_file_path.exist?
    end

    private def lock
      FileUtils.touch(lock_file_path)
    end

    private def unlock
      FileUtils.rm_f(lock_file_path)
    end

    private def lock_file_path
      Pathname.new("#{@path}.LOCK")
    end

    def unlock_help(jobnet)
      "remove the file: #{lock_file_path}"
    end

    def cancel_jobnet(jobnet, message)
      unlock
      FileUtils.rm_f(@path)
      @queue.clear
    end

    class Task
      def initialize(job)
        @job = job
      end

      attr_reader :job

      def serialize
        [@job].join("\t")
      end

      def Task.deserialize(str)
        job, * = str.strip.split("\t")
        new(JobNet::Ref.parse(job))
      end
    end

  end


  class DatabaseTaskQueue

    def initialize(datasource:, executor_id:, enable_lock: false)
      @ds = datasource
      @executor_id = executor_id
      @enable_lock = enable_lock

      @queue = []
      @jobnet_dao = DAO::JobNet.new(@ds)
      @job_dao = DAO::Job.new(@ds)
      @jobexecution_dao = DAO::JobExecution.new(@ds)
      @jobnet = nil
    end

    def empty?
      @queue.empty?
    end

    def size
      @queue.size
    end

    private def find_or_create_jobnet(ref)
      @jobnet_rec ||= @jobnet_dao.find_or_create(ref)
    end

    def restore_jobnet(jobnet)
      raise "jobnet is already bound to queue" if @jobnet

      job_executions = @jobexecution_dao.enqueued_jobs(jobnet.ref)
      unless job_executions.empty?
        job_executions.each do |job_execution|
          @queue.push Task.for_job_execution(job_execution)
        end
        @jobnet = jobnet
      end
    end

    def enqueue_jobnet(jobnet)
      raise "jobnet is already bound to queue" if @jobnet

      jobnet_rec = find_or_create_jobnet(jobnet.ref)
      jobnet.sequential_jobs.each_with_index do |job_ref, index|
        job = @job_dao.find_or_create(jobnet_rec.id, job_ref)
        job_execution = @jobexecution_dao.enqueue_job(job, index + 1)
        @queue.push Task.for_job_execution(job_execution)
      end
      @jobnet = jobnet
    end

    def each
      @queue.each do |task|
        yield task.job
      end
    end

    def consume_each
      raise "jobnet is not bound to queue" unless @jobnet

      jobnet_rec = find_or_create_jobnet(@jobnet.ref)
      @jobnet_dao.lock(jobnet_rec.id, @executor_id) if @enable_lock
      while task = @queue.first
        @job_dao.lock(task.job_id, @executor_id) if @enable_lock
        begin
          @jobexecution_dao.transition_to_running(task.job_execution_id)

          # Note: fork(2) breaks current connections,
          # we must close current connections before fork.
          # (psql datasource forks process)
          @ds.clear_connection_pool

          job_completed = false
          begin
            task_result = yield task.job

            if task_result.success?
              @jobexecution_dao.transition_to_succeeded(task.job_execution_id)
              job_completed = true
              @queue.shift
            else
              @jobexecution_dao.transition_to_failed(task.job_execution_id, task_result.message)
              job_completed = true
              break
            end
          ensure
            unless job_completed
              begin
                @jobexecution_dao.transition_to_failed(task.job_execution_id, 'unexpected error')
              rescue => ex
                $stderr.puts "warning: could not write job state: #{ex.class}: #{ex.message} (this error is ignored)"
              end
            end
          end
        ensure
          @job_dao.unlock(task.job_id, @executor_id) if @enable_lock
        end
      end
    ensure
      @jobnet_dao.unlock(jobnet_rec.id, @executor_id) if @enable_lock
    end

    def locked?(jobnet)
      @jobnet_dao.locked?(jobnet.ref)
    end

    def unlock_help(jobnet)
      jobnet_rec = find_or_create_jobnet(jobnet.ref)
      locked_jobs = @job_dao.locked_jobs(jobnet_rec.id)
      "clear executor_id of the jobnet (id: #{jobnet_rec.id}) and/or the jobs (id: #{locked_jobs.map(&:id).join(', ')})"
    end

    def cancel_jobnet(jobnet, message)
      @jobexecution_dao.cancel_jobnet(jobnet.ref, message)
      jobnet_rec = find_or_create_jobnet(jobnet.ref)
      @jobnet_dao.clear_lock(jobnet_rec.id)
      @job_dao.clear_lock_all(jobnet_rec.id)
    end

    class Task
      def Task.for_job_execution(exec)
        job_ref = JobNet::JobRef.new(exec.subsystem, exec.job_name, JobNet::Location.dummy)
        new(job_ref, exec)
      end

      def initialize(job_ref, job_execution)
        @job = job_ref
        @job_id = job_execution.job_id
        @job_execution_id = job_execution.job_execution_id
      end

      attr_reader :job
      attr_reader :job_id
      attr_reader :job_execution_id
    end

  end

end
