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

  class TaskQueue
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
      lock
      save
      while task = self.next
        task_result = yield task
        break unless task_result.success?
        dequeue
      end
    ensure
      unlock
    end

    def enqueue(task)
      @queue.push task
    end

    def next
      @queue.first
    end

    def dequeue
      task = @queue.shift
      save
      task
    end

    def save
    end

    def restore
    end

    def locked?
      false
    end

    def lock
    end

    def unlock
    end

    def unlock_help
      "[MUST NOT HAPPEN] this message must not be shown"
    end
  end

  class FileTaskQueue < TaskQueue
    def FileTaskQueue.restore_if_exist(path)
      q = new(path)
      q.restore if q.queued?
      q
    end

    def initialize(path)
      super()
      @path = path
    end

    def queued?
      @path.exist?
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
          each do |task|
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

    def lock
      FileUtils.touch lock_file_path
    end

    def unlock
      FileUtils.rm_f lock_file_path
    end

    def lock_file_path
      Pathname.new("#{@path}.LOCK")
    end

    def unlock_help
      "remove the file: #{lock_file_path}"
    end
  end

  class DatabaseTaskQueue < TaskQueue
    def DatabaseTaskQueue.restore_if_exist(datasource, jobnet_ref)
      subsys, jobnet_name = jobnet_ref.name.delete('*').split('/')
      job_refs = jobnet_ref.refs - [jobnet_ref.start, *jobnet_ref.net_refs, jobnet_ref.end]
      jobnet = Bricolage::DAO::JobNet.new(datasource).find_or_create(subsys, jobnet_name)
      job_dao = Bricolage::DAO::Job.new(datasource)
      jobs = job_refs.map {|jobref| job_dao.find_or_create(subsys, jobref.name, jobnet.id) }

      q = new(datasource, subsys, jobnet_name, jobs)

      q.restore
      q.enqueue_job_executions unless q.queued?
      q
    end

    def initialize(datasource, subsys, jobnet_name, jobs)
      super()
      @ds = datasource
      @subsys = subsys
      @jobnet_name = jobnet_name
      @jobs = jobs

      @jobexecution_dao = Bricolage::DAO::JobExecution.new(@ds)
    end

    def queued?
      !@queue.empty?
    end

    def consume_each
      lock
      while task = self.next
        dequeuing

        @ds.clear_connection_pool
        task_result = yield task # running execute_job

        if task_result.success?
          dequeued
        else
          @jobexecution_dao.update(where: {subsystem: task.subsystem, job_name: task.job_name},
                                   set:   {status: 'failed', message: task_result.message})
          break
        end
      end
    ensure
      unlock
    end

    def enqueue(task)
      @jobexecution_dao.update(where: {'j.subsystem': task.subsystem, job_name: task.job_name},
                               set:   {status: 'waiting', message: nil, submitted_at: :now, started_at: nil, finished_at: nil})
      @queue.push task
    end

    def dequeuing
      task = @queue.first
      @jobexecution_dao.update(where: {'j.subsystem': task.subsystem, job_name: task.job_name},
                               set:   {status: 'running', started_at: :now})
      task
    end

    def dequeued
      task = @queue.shift
      @jobexecution_dao.update(where: {'j.subsystem': task.subsystem, job_name: task.job_name},
                               set:   {status: 'succeeded', finished_at: :now})
      task
    end

    def restore
      job_executions = @jobexecution_dao.where('j.subsystem': @jobs.map(&:subsystem).uniq,
                                               jobnet_name: @jobnet.jobnet_name,
                                               status: ['waiting', 'running', 'failed'])

      job_executions.each do |je|
        enqueue JobTask.for_job_execution(je)
      end
    end

    def enqueue_job_executions
      @jobs.each do |job|
        @jobexecution_dao.upsert(set: {status: 'waiting', job_id: job.id, message: nil, lock: false})
      end
    end

    def locked?
      count = @jobexecution_dao
                .where(subsystem: @subsys, jobnet_name: @jobnet_name, lock: true)
                .count
      count > 0
    end

    def lock
      @jobexecution_dao.update(where: {subsystem: @subsys, jobnet_name: @jobnet_name},
                               set:   {lock: true})
    end

    def unlock
      @jobexecution_dao.update(where: {subsystem: @subsys, jobnet_name: @jobnet_name},
                               set:   {lock: false})
    end

    def locked_records
      @jobexecution_dao.where(subsystem: @subsys, jobnet_name: @jobnet_name, lock: true)
    end

    def unlock_help
      "remove the id records from job_executions: #{locked_records.map(&:job_execution_id)}"
    end

    # for debug to test
    def clear
      @jobexecution_dao.update(where: {subsystem: @subsys, jobnet_name: @jobnet_name},
                               set:   {status: 'succeeded'})
    end
  end

  class JobTask
    def initialize(job)
      @job = job
      @job_name = job.name
      @subsystem = job.subsystem
    end

    attr_reader :job
    attr_reader :subsystem
    attr_reader :job_name

    def serialize
      [@job].join("\t")
    end

    def JobTask.deserialize(str)
      job, * = str.strip.split("\t")
      new(JobNet::Ref.parse(job))
    end

    def JobTask.for_job_execution(job_execution)
      new(JobNet::JobRef.new(job_execution.subsystem, job_execution.job_name, JobNet::Location.dummy))
    end
  end

end
