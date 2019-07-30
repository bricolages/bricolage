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
      datasource = context.get_data_source('psql', 'test_db')

      jobnet_id = JobNet.find_or_create(jobnet.name, datasource)
      jobs = jobnet.refs - [jobnet.start, *jobnet.net_refs, jobnet.end]
      jobs.each {|job| Job.create(jobnet_id, job.to_s, datasource) }

      q = new(datasource, jobnet, jobs)

      q.restore
      q.allocate unless q.queued?
      q
    end

    def initialize(datasource, jobnet, jobs)
      super()
      @ds = datasource
      @jobnet = jobnet
      @subsys, @name = @jobnet.name.split('/')
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
      @ds.open do |conn| conn.query_values(<<~SQL)
        UPDATE
          job_executions
        SET
          status = 'waiting',
          submitted_at = NOW(),
          started_at = null,
          finished_at = null
        FROM
          job_executions je
          JOIN jobs j using(job_id)
        WHERE
          j.subsystem = '#{subsys}'
          AND j.job_name = '#{name}'
        ;
        SQL
      end
      @queue.push task
    end

    def dequeuing
      task = @queue.first
      subsys, name = task.job.to_s.split('/')
      @ds.open do |conn| conn.query_values(<<~SQL)
        UPDATE
          job_executions
        SET
          status = 'running',
          started_at = NOW()
        FROM
          job_executions je
          JOIN jobs j using(job_id)
        WHERE
          j.subsystem = '#{subsys}'
          AND j.job_name = '#{name}'
        ;
        SQL
      end
      task
    end

    def dequeued
      task = @queue.shift
      subsys, name = task.job.to_s.split('/')
      @ds.open do |conn| conn.query_values(<<~SQL)
        UPDATE
          job_executions
        SET
          status = 'succeeded',
          finished_at = NOW()
        FROM
          job_executions je
          JOIN jobs j using(job_id)
        WHERE
          j.subsystem = '#{subsys}'
          AND j.job_name = '#{name}'
        ;
        SQL
      end
      task
    end

    def save
      if @queue.empty?
         clear
         return
      end

      @ds.open do |conn|
        conn.transaction {
          @queue.each do |job_task|
            subsys, name = job_task.job.to_s.split('/')
            conn.query_value(<<~SQL)
              UPDATE
                job_executions
              SET
                status = 'running'
              FROM
                job_executions je
                JOIN jobs j using(job_id)
              WHERE
                j.subsystem = '#{subsys}'
                AND j.job_name = '#{name}'
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
          and (je.status = 'running' or je.status = 'failed')
        ;
        SQL
      end

      job_lines.each do |line|
        enq JobTask.deserialize(line)
      end
    end

    def allocate
      # job execution insert or update
      @ds.open do |conn|
        conn.transaction {
          @jobs.each do |job|
            subsys, name = job.to_s.split('/')
            conn.execute(<<~SQL)
              INSERT INTO  job_executions (status, job_id, lock, started_at)
              SELECT 'waiting', job_id, false, NOW()
              FROM jobs
              WHERE subsystem='#{subsys}' AND job_name='#{name}'
              ON CONFLICT  DO NOTHING
              ;
            SQL
          end
        }
      end


      # job execution state
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
          and je.lock
        ;
        SQL
      end
      count.to_i > 0
    end

    def lock
      @ds.open do |conn| conn.query_values(<<~SQL)
        UPDATE
          job_executions
        SET
          lock = true
        FROM
          job_executions je
          JOIN jobs j using(job_id)
          JOIN jobnets jn using(jobnet_id)
        WHERE
          jn.subsystem = '#{@subsys}'
          AND jn.jobnet_name = '#{@name}'
        ;
        SQL
      end
    end

    def unlock
      @ds.open do |conn| conn.query_values(<<~SQL)
        UPDATE
          job_executions
        SET
          lock = false
        FROM
          job_executions je
          JOIN jobs j using(job_id)
          JOIN jobnets jn using(jobnet_id)
        WHERE
          jn.subsystem = '#{@subsys}'
          AND jn.jobnet_name = '#{@name}'
        ;
        SQL
      end
    end

    def lock_records
      @ds.open do |conn| conn.query_values(<<~SQL)
        SELECT
          job_execution_id
        FROM
          job_executions je
          JOIN jobs j using(job_id)
          JOIN jobnets jn using(jobnet_id)
        WHERE
          jn.subsystem = '#{@subsys}'
          AND jn.jobnet_name = '#{@name}'
          AND je.lock
        ;
        SQL
      end
    end

    def unlock_help
      "remove the id records from job_executions: #{lock_records}"
    end

    # for debug to test
    def clear
      @ds.open do |conn| conn.execute(<<~SQL)
        UPDATE
          job_executions
        SET
          status = 'succeeded'
        FROM
          job_executions je
          JOIN jobs j using(job_id)
          JOIN jobnets jn using(jobnet_id)
        WHERE
          jn.subsystem = '#{@subsys}'
          AND jn.jobnet_name = '#{@name}'
        ;
        SQL
      end
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
