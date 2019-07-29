require 'bricolage/jobnet'
require 'bricolage/sqlutils'
require 'bricolage/exception'
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
    def DatabaseTaskQueue.restore_if_exist(context, jobnet)
      q = new(context, jobnet)
      q.restore
      q
    end

    def initialize(context, jobnet)
      super()
      @ctx = context
      @ds = @ctx.get_data_source('psql', 'test_db')
      @jobnet = jobnet
      @subsys, @name = @jobnet.name.split('/')
    end

    def queued?
      !@queue.empty?
    end

    def save
      if @queue.empty?
         clear
         return
      end

      @ds.open do |conn|
        conn.transaction {
          @queue.each do |job_task|
            conn.query_value(<<~SQL)
              INSERT INTO  job_executions (status, job_id, started_at)
              VALUES ('unlock', 1, NOW())
              ON CONFLICT (job_id) DO NOTHING
              --on conflict on constraint job_execution_fk_job
              --do update set status=''
              ;
            SQL
          end
        }
      end
    end

    def restore
      job_lines = @ds.open do |conn| conn.query_values(<<~SQL)
        select
          jn.subsystem || '/' || jn.jobnet_name
        from
          job_executions je
          join jobs j using(job_id)
          join jobnets jn using(jobnet_id)
        where
          jn.subsystem = '#{@subsys}'
          and jn.jobnet_name = '#{@name}'
          and je.finished_at is null
          and je.status <> 'succeeded'
        ;
        SQL
      end

      job_lines.each do |line|
        enq JobTask.deserialize(line)
      end
    end

    def clear
      @ds.open do |conn| conn.execute(<<~SQL)
        delete
        from
          job_executions
        using
          jobs, jobnets
        where
          job_executions.job_id = jobs.job_id
          and jobs.jobnet_id = jobnets.jobnet_id
          and jobnets.subsystem = '#{@subsys}'
          and jobnets.jobnet_name = '#{@name}'
        ;
        SQL
      end

    end

    def locked?
      count = @ds.open do |conn| conn.query_value(<<~SQL)
        select
          count(1)
        from
          job_executions je
          join jobs j using(job_id)
          join jobnets jn using(jobnet_id)
        where
          jn.subsystem = '#{@subsys}'
          and jn.jobnet_name = '#{@name}'
          and je.status = 'lock'
        ;
        SQL
      end
      count.to_i > 0
    end

    def lock
      @ds.open do |conn| conn.query_values(<<~SQL)
        update
          job_executions
        set
          status = 'lock'
        from
          job_executions je
          join jobs j using(job_id)
          join jobnets jn using(jobnet_id)
        where
          jn.subsystem = '#{@subsys}'
          and jn.jobnet_name = '#{@name}'
        ;
        SQL
      end
    end

    def unlock
      @ds.open do |conn| conn.query_values(<<~SQL)
        update
          job_executions
        set
          status = 'unlock'
        from
          job_executions je
          join jobs j using(job_id)
          join jobnets jn using(jobnet_id)
        where
          jn.subsystem = '#{@subsys}'
          and jn.jobnet_name = '#{@name}'
        ;
        SQL
      end
    end

    def lock_records
      @ds.open do |conn| conn.query_values(<<~SQL)
        select
          job_execution_id
        from
          job_executions je
          join jobs j using(job_id)
          join jobnets jn using(jobnet_id)
        where
          jn.subsystem = '#{@subsys}'
          and jn.jobnet_name = '#{@name}'
          and status = 'lock'
        ;
        SQL
      end
    end

    def unlock_help
      "remove the id records from job_executions: #{lock_records}"
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
