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

    def queued?
      not empty?
    end

    def each(&block)
      @queue.each(&block)
    end

    def consume_each
      while task = @queue.first
        result = yield task
        break unless result.success?
        @queue.shift
      end
    end

    def enqueue(task)
      @queue.push task
    end

    def locked?
      false
    end

    def unlock_help
      raise "[BUG] this message must not be shown"
    end

  end


  class FileTaskQueue

    def FileTaskQueue.restore_if_exist(path)
      q = new(path)
      q.restore if q.queued?
      q
    end

    def initialize(path)
      @path = path
      @queue = []
    end

    def empty?
      @queue.empty?
    end

    def size
      @queue.size
    end

    def queued?
      @path.exist?
    end

    def each
      @queue.each do |task|
        yield task
      end
    end

    def consume_each
      lock
      save
      while task = @queue.first
        task_result = yield task
        break unless task_result.success?
        @queue.shift
        save
      end
    ensure
      unlock
    end

    def save
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

    def restore
      File.foreach(@path) do |line|
        enqueue JobTask.deserialize(line)
      end
    end

    def locked?
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

    def unlock_help
      "remove the file: #{lock_file_path}"
    end

  end


  class DatabaseTaskQueue

    def DatabaseTaskQueue.restore_if_exist(datasource, jobnet_ref, executor_id)
      jobnet_subsys, jobnet_name = jobnet_ref.start_jobnet.name.delete('*').split('/')
      job_refs = jobnet_ref.sequential_jobs

      q = new(datasource, jobnet_subsys, jobnet_name, job_refs, executor_id)

      return q if q.locked?

      q.restore
      q.enqueue_job_executions unless q.queued?
      q
    end

    def DatabaseTaskQueue.clear_queue(datasource, jobnet_ref)
      jobnet_subsys, jobnet_name = jobnet_ref.start_jobnet.name.delete('*').split('/')
      job_refs = jobnet_ref.sequential_jobs

      q = new(datasource, jobnet_subsys, jobnet_name, job_refs, nil)
      q.clear
    end

    def initialize(datasource, subsys, jobnet_name, job_refs, executor_id)
      @ds = datasource
      @executor_id = executor_id

      @queue = []
      @jobnet_dao = Bricolage::DAO::JobNet.new(@ds)
      @job_dao = Bricolage::DAO::Job.new(@ds)
      @jobexecution_dao = Bricolage::DAO::JobExecution.new(@ds)

      @jobnet_obj = jobnet
      @jobnet = find_jobnet(jobnet)
      @jobs = job_refs.map {|jobref| @job_dao.find_or_create(jobref.subsystem.to_s, jobref.name, @jobnet.id) }
    end

    def empty?
      @queue.empty?
    end

    def size
      @queue.size
    end

    private def find_jobnet(jobnet)
      @jobnet_dao.find_or_create(jobnet.ref)
    end

    def each
      @queue.each do |task|
        yield task
      end
    end

    def consume_each
      lock_jobnet

      while task = @queue.first
        lock_job(task)
        begin
          job_executions = @jobexecution_dao.update(
            where: {job_execution_id: task.job_execution_id},
            set: {status: Bricolage::DAO::JobExecution::STATUS_RUN, started_at: :now}
          )

          # Note: fork(2) breaks current connections,
          # we must close current connections before fork.
          # (psql datasource forks process)
          @ds.clear_connection_pool

          task_result = yield task.job

          if task_result.success?
            @jobexecution_dao.update(
              where: {job_execution_id: task.job_execution_id},
              set: {status: Bricolage::DAO::JobExecution::STATUS_SUCCESS, finished_at: :now}
            )
            @queue.shift
          else
            @jobexecution_dao.update(
              where: {job_execution_id: task.job_execution_id},
              set: {status: Bricolage::DAO::JobExecution::STATUS_FAILURE, message: task_result.message}
            )
            break
          end
        ensure
          unlock_job(task)
        end
      end
    ensure
      unlock_jobnet
    end

    def enqueue(job_execution)
      job_execution_id = job_execution.job_execution_id
      @jobexecution_dao.update(where: {job_execution_id: job_execution_id},
                               set:   {status: Bricolage::DAO::JobExecution::STATUS_WAIT,
                                       message: nil, submitted_at: :now, started_at: nil, finished_at: nil})
      @queue.push JobExecutionTask.for_job_execution(job_execution)
    end

    def restore
      job_executions = @jobexecution_dao.where('jn.subsystem': @jobnet.subsystem,
                                               'jn.jobnet_name': @jobnet.jobnet_name,
                                               status: [Bricolage::DAO::JobExecution::STATUS_WAIT,
                                                        Bricolage::DAO::JobExecution::STATUS_RUN,
                                                        Bricolage::DAO::JobExecution::STATUS_FAILURE])
      job_executions
        .sort_by { |je| je.execution_sequence.to_i }
        .each { |je| enqueue(je) }
    end

    def enqueue_job_executions
      @jobs.each_with_index do |job, index|
        job_execution = @jobexecution_dao.create(job.id, index, Bricolage::DAO::JobExecution::STATUS_WAIT)
        job_execution.job_name = job.job_name
        job_execution.subsystem = job.subsystem
        @queue.push JobExecutionTask.for_job_execution(job_execution)
      end
    end

    def locked?
      jobnet_lock = @jobnet_dao.check_lock(@jobnet.id)
      jobs_lock = @job_dao.check_lock(@jobs.map(&:id))
      jobnet_lock || jobs_lock
    end

    def unlock_help
      jobnet_rec = find_jobnet(@jobnet_obj)
      @job_dao.where(jobnet_id: jobnet_rec.id).reject {|job| job.executor_id.nil? }
      "update the job_id records to unlock from job tables: #{locked_jobs.map(&:id)}"
    end

    private def lock_job(task)
      return   # FIXME: tmp

      raise "Invalid job_id" if task.job_id.nil?
      lock_results = @job_dao.update(where: {job_id: task.job_id, executor_id: nil},
                                     set:   {executor_id: @executor_id})
      if lock_results.empty?
        raise DoubleLockError, "Already locked id:#{job_id} job"
      end
    end

    private def lock_jobnet
      return   # FIXME: tmp

      lock_results = @jobnet_dao.update(where: {jobnet_id: @jobnet.id, executor_id: nil},
                                        set:   {executor_id: @executor_id})
      if lock_results.empty?
        raise DoubleLockError, "Already locked id:#{@jobnet.id} jobnet"
      end
    end

    private def unlock_job(task)
      return   # FIXME: tmp

      @job_dao.update(where: {job_id: task.job_id},
                      set:   {executor_id: nil})
    end

    private def unlock_jobnet
      return   # FIXME: tmp

      @jobnet_dao.update(where: {jobnet_id: @jobnet.id},
                         set:   {executor_id: nil})
    end

    def clear
      @jobexecution_dao.update(where: {'je.job_id': @jobs.map(&:id)},
                               set:   {status: Bricolage::DAO::JobExecution::STATUS_CANCEL})
    end

    # only for idempotence of test, NOT use for jobnet command
    def reset
      @jobexecution_dao.delete('je.job_execution_id': @queue.map(&:job_execution_id))
      @job_dao.delete(job_id: @jobs.map(&:id))
      @jobnet_dao.delete(jobnet_id: @jobnet.id)
    end
  end

  class JobTask
    def initialize(job)
      @job = job
    end

    attr_reader :job

    def serialize
      [@job].join("\t")
    end

    def JobTask.deserialize(str)
      job, * = str.strip.split("\t")
      new(JobNet::Ref.parse(job))
    end
  end

  class JobExecutionTask
    def initialize(job, job_execution)
      @job = job
      @job_name = job.name
      @subsystem = job.subsystem.to_s
      @job_id = job_execution.job_id
      @job_execution_id = job_execution.job_execution_id
    end

    attr_reader :job
    attr_reader :job_name
    attr_reader :subsystem
    attr_reader :job_id
    attr_reader :job_execution_id

    def JobExecutionTask.for_job_execution(job_execution)
      jobref = JobNet::JobRef.new(job_execution.subsystem, job_execution.job_name, JobNet::Location.dummy)
      new(jobref, job_execution)
    end
  end

end
