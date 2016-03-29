require 'bricolage/exception'
require 'securerandom'
require 'json'
require 'forwardable'

module Bricolage

  module StreamingLoad

    class Loader

      def Loader.main
        config_path, task_id = ARGV

        config = YAML.load(File.read(config_path))

        ctx = Context.for_application('.')
        loader = new(
          data_source: ctx.get_data_source('sql', 'sql'),
          ctl_bucket: config['ctl_bucket'],
          logger: ctx.logger
        )
        loader.execute_task task_id
      end

      def initialize(data_source:, ctl_bucket:, logger:)
        @ds = ds
        @ctl_bucket = ctl_bucket
        @logger = logger
      end

      def execute_task(task_id)
        @ds.open {|conn|
          job = LoadJob.new(
            conn,
            task_id: task_id,
            ctl_bucket: @ctl_bucket,
            logger: @logger
          )
          job.run
        }
      end

    end


    class LoadJob

      def initialize(connection, task_id:, ctl_bucket:, logger:, noop: false)
        @connection = connection
        @task_id = task_id
        @ds = data_source
        @ctl_bucket = ctl_bucket
        @logger = logger
        @noop = noop

        @job_id = SecureRandom.uuid
        @job_seq = nil
        @start_time = Time.now
        @end_time = nil
      end

      attr_reader :sql

      def run
        task = LoadTask.load(@connection, @task_id)
        check_table_conflict task
        assign_task task
        task.lock {
          unless task.empty?
            if task.use_work_table?
              load_objects task.work_table, task
              @connection.transaction {
                commit_work_table task
                commit_job_result
              }
            else
              @connection.transaction {
                load_objects task.dest_table, task
                commit_job_result
              }
            end
          end
        }
      end

      def check_table_conflict(task)
        # FIXME: check target table is not locked
      end

      def assign_task(task)
        @connection.execute(<<-EndSQL)
            insert into dwh_jobs
                ( dwh_job_id
                , job_class
                , utc_start_time
                , os_pid
                , dwh_task_seq
                )
            values
                ( #{s @job_id}
                , 'streaming_load_v3'
                , #{t @start_time.getutc}
                , #{$$}
                , #{task.task_seq}
                )
            ;
        EndSQL
        @job_seq = @connection.query_value(<<-EndSQL)
            select dwh_job_seq
            from dwh_jobs
            where dwh_job_id = #{s @job_id}
            ;
        EndSQL
      end

      def load_objects(dest_table, task)
        ManifestFile.create(task, @ctl_bucket, @logger) {|manifest|
          @connection.execute(<<-";".strip.gsub(/\s+/, ' '))
            copy #{dest_table}
            from '#{manifest.url}'
            credentials '#{manifest.credential_string}'
            manifest
            statupdate false
            #{task.options}
          ;
          @logger.info "load succeeded: #{manifest.url}" unless @noop
        }
      end

      def commit_work_table(task)
        insert_stmt = @sql ? @sql.source : "insert into #{@table} select * from #{work_table};"
        @connection.execute(insert_stmt)
        # keep work table records for later tracking
      end

      def commit_job_result
        @end_time = Time.now
        write_job_result 'success', ''
      end

      def write_job_result(status, message)
        @connection.execute(<<-EndSQL)
          insert into dwh_job_results
              ( dwh_job_seq
              , utc_end_time
              , status
              , message
          values
              ( #{@job_seq}
              , #{t @end_time.getutc}
              , #{s status}
              , #{s message}
              )
          ;
        EndSQL
      end

      def sql_timestamp(time)
        %Q(timestamp '#{format_timestamp(time)}')
      end

      def format_timestamp(time)
        time.strftime('%Y-%m-%d %H:%M:%S')
      end

      def sql_string(str)
        escaped = str.gsub("'", "''")
        %Q('#{escaped}')
      end

    end


    class LoadTask

      def LoadTask.load(conn, task_id)
        rec = conn.query_row(<<-EndSQL)
          select *
          from dwh_tasks
          where dwh_task_id = '#{task_id}'
          ;
        EndSQL
        new(
          conn,
          task_seq: rec['dwh_task_seq'],
          task_id: rec['dwh_task_id'],
          task_class: rec['dwh_task_class'],
          schema: rec['schema_name'],
          table: rec['table_name']
        )
      end

      def initialize(conn, task_seq:, task_id:, task_class:, schema:, table:)
        @connection = conn
        @task_seq = task_seq
        @task_id = task_id
        @task_class = task_class
        @schema = schema
        @table = table
      end

      def dest_table
        "#{@schema}.#{table}"
      end

      def lock(job_seq)
        @connection.execute(<<-EndSQL)
            insert into dwh_str_load_files
                ( dwh_job_seq
                , object_url
                )
            select
                #{job_seq}
                , object_url
            from
                dwh_str_load_files_incoming
            where
                dwh_task_seq = #{@task_seq}
                and object_url not in (
                    select
                        f.object_url
                    from
                        dwh_str_load_files f
                        inner join dwh_job_results r using (dwh_job_seq)
                    where
                        r.status = 'success'
                )
            ;
        EndSQL
      end

    end


    class ManifestFile

      def ManifestFile.create(task:, ctl_bucket:, logger:, noop: false)
        manifest = new(task, ctl_bucket, noop: noop)
        logger.info "creating manifest: #{manifest.name}"
        manifest.put
        logger.debug "manifest:\n" + manifest.content if logger.debug?
        yield manifest
        #manifest.delete
      end

      def initialize(task, ctl_bucket, noop: false)
        @task = task
        @ctl_bucket
        @noop = noop
        @url = nil
      end

      def credential_string
        @ctl_bucket.credential_string
      end

      def name
        @name ||= "manifest-#{@task.dwh_proc_id}.json"
      end

      def content
        json = make_manifest_json(task.target_objects)
      end

      def put
        @url = @ctl_bucket.put(@name, content, noop: @noop)
      end

      def delete
        @ctl_bucket.delete(@name, noop: @noop)
      end

      def content
        @content ||= begin
          ents = @task.target_objects.map {|obj|
            { "url" => obj.url, "mandatory" => false }
          }
          obj = { "entries" => ents }
          JSON.pretty_generate(obj)
        end
      end

    end


    class ControlConnection

      def ControlConnection.open(ds, logger)
        ds.open {|conn|
          yield new(ds, conn, logger)
        }
      end

      def initialize(ds, conn, logger)
        @ds = ds
        @connection = conn
        @logger = logger
      end

      def transaction(&block)
        @connection.transaction(&block)
      end

      def execute(query)
        if @noop
          @connection.log_query(query)
        else
          @connection.update(query)
        end
      end

      def query(query)
        @connection.query(query) {|rs| rs.to_a }
      end

    end

  end

end
