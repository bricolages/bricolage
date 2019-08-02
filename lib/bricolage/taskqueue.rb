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
        yield task
        deq
      end
    ensure
      unlock
    end

    def enq(task)
      @queue.push task
    end

    def next
      @queue.first
    end

    def deq
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
        enq JobTask.deserialize(line)
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
    def DatabaseTaskQueue.restore_if_exist(context, jobnet_ref)
      datasource = context.get_data_source('psql', 'test_db')

      subsys, jobnet_name = jobnet_ref.name.delete('*').split('/')
      job_refs = jobnet_ref.refs - [jobnet_ref.start, *jobnet_ref.net_refs, jobnet_ref.end]
      jobnet = Bricolage::DAO::JobNet.new(datasource).find_or_create(subsys, jobnet_name)
      jobs = job_refs.map do |jobref|
        Bricolage::DAO::Job.new(datasource).find_or_create(subsys, jobref.name, jobnet.id)
      end

      q = new(datasource, subsys, jobnet_name, jobs)

      q.restore
      q.allocate unless q.queued?
      q
    end

    def initialize(datasource, subsys, jobnet_name, jobs)
      super()
      @ds = datasource
      @subsys = subsys
      @jobnet_name = jobnet_name
      @jobs = jobs
    end

    def queued?
      !@queue.empty?
    end

    def consume_each
      lock
      while task = self.next
        dequeuing
        yield task
        dequeued
      end
    ensure
      unlock
    end

    def enq(task)
      subsys, name = task.job.to_s.split('/')

      Bricolage::DAO::JobExecution.new(@ds)
        .update(where: {subsystem: subsys, job_name: name},
                set:   {status: 'waiting', submitted_at: :now, started_at: nil, finished_at: nil})
      @queue.push task
    end

    def dequeuing
      task = @queue.first
      subsys, name = task.job.to_s.split('/')

      Bricolage::DAO::JobExecution.new(@ds)
        .update(where: {subsystem: subsys, job_name: name},
                set:   {status: 'running', started_at: :now})
      task
    end

    def dequeued
      task = @queue.shift
      subsys, name = task.job.to_s.split('/')

      Bricolage::DAO::JobExecution.new(@ds)
        .update(where: {subsystem: subsys, job_name: name},
                set:   {status: 'succeeded', finished_at: :now})
      task
    end

    def restore
      job_executions = Bricolage::DAO::JobExecution.new(@ds)
                         .find_by(subsystem: @subsys,
                                  jobnet_name: @jobnet_name,
                                  finished_at: nil,
                                  status: ['running', 'failed'])

      job_executions.each do |je|
        line = "#{je.subsystem}/#{je.job_name}"
        enq JobTask.deserialize(line)
      end
    end

    def allocate
      @jobs.each do |job|
        Bricolage::DAO::JobExecution.new(@ds)
          .upsert(where:  {subsystem: job.subsystem, job_name: job.job_name},
                  set: {status: 'waiting', job_id: job.id, lock: false})
      end
    end

    def locked?
      count = Bricolage::DAO::JobExecution.new(@ds)
                .find_by(subsystem: @subsys, jobnet_name: @jobnet_name, lock: true)
                .count
      count > 0
    end

    def lock
      Bricolage::DAO::JobExecution.new(@ds)
        .update(where: {subsystem: @subsys, jobnet_name: @jobnet_name},
                set:   {lock: true})
    end

    def unlock
      Bricolage::DAO::JobExecution.new(@ds)
        .update(where: {subsystem: @subsys, jobnet_name: @jobnet_name},
                set:   {lock: false})
    end

    def lock_records
      Bricolage::DAO::JobExecution.new(@ds)
        .find_by(subsystem: @subsys, jobnet_name: @jobnet_name, lock: true)
    end

    def unlock_help
      "remove the id records from job_executions: #{lock_records}"
    end

    # for debug to test
    def clear
      Bricolage::DAO::JobExecution.new(@ds)
        .update(where: {subsystem: @subsys, jobnet_name: @jobnet_name},
                set:   {status: 'succeeded'})
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

end
