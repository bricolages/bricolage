require 'bricolage/sqlutils'
require 'bricolage/exception'
require 'securerandom'
require 'json'
require 'forwardable'

module Bricolage

  module StreamingLoad

    class Loader

      def Loader.main
require 'pp'
require 'pry'
        config_path, task_id = ARGV

        config = YAML.load(File.read(config_path))
pp config

        ctx = Context.for_application('.')
        loader = new(
          data_source: ctx.get_data_source('sql', config['redshift-ds']),
          ctl_bucket: ctx.get_data_source('s3', config['s3-ds']),
          logger: ctx.logger
        )
        loader.execute_task task_id
      end

      def initialize(data_source:, ctl_bucket:, logger:)
        @ds = data_source
        @ctl_bucket = ctl_bucket
        @logger = logger

        @noop = false
      end

      def execute_task(task_id)
        LoaderConnection.open(@ds, logger: @logger, noop: @noop) {|conn|
          job = LoadJob.new(conn, ctl_bucket: @ctl_bucket, logger: @logger)
          job.process(task_id)
        }
      end

    end


    class LoadJob

      include SQLUtils

      def initialize(connection, ctl_bucket:, logger:, noop: false)
        @connection = connection
        @ctl_bucket = ctl_bucket
        @logger = logger
        @noop = noop

        @job_id = SecureRandom.uuid
        @job_seq = nil
        @start_time = Time.now
        @end_time = nil
      end

      attr_reader :sql

      def process(task_id)
        task = LoadTask.load(@connection, task_id)
        assign_task task
        begin
          staged_files = stage_files(task)
          return if staged_files.empty?
          ManifestFile.create(
            @ctl_bucket,
            job_id: @job_id,
            object_urls: staged_files,
            logger: @logger,
            noop: @noop
          ) {|manifest|
load_options = %q(json 'auto' gzip timeformat 'auto' truncatecolumns acceptinvchars ' ')    # FIXME: read from table
            if task.use_work_table?
              load_objects task.work_table, manifest, load_options
              @connection.transaction {
                commit_work_table task
                commit_job_result
              }
            else
              @connection.transaction {
                load_objects task.dest_table, manifest, load_options
                commit_job_result
              }
            end
          }
        rescue JobFailure => ex
          abort_job 'failure', ex.message
          raise
        rescue Exception => ex
          abort_job 'error', ex.message
          raise
        end
      end

      def assign_task(task)
        success = true
        @connection.transaction {
          @connection.lock('dwh_tasks')
          @connection.lock('dwh_jobs')
          @connection.lock('dwh_job_results')

          processed = task_processed?(task)
          table_in_use = table_in_use?(task)
          write_job(task.task_seq)
          @job_seq = read_job_seq(@job_id)
          @logger.info "dwh_job_seq=#{@job_seq}"
          if processed
            abort_job 'failure', "task is already processed by other jobs"
            success = false
          end
          if table_in_use
            abort_job 'failure', "other jobs are working on the target table"
            success = false
          end
        }
        raise JobFailure, "could not assign task #{task.task_id} to the job" unless success
      end

      def write_job(task_seq)
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
                , #{task_seq}
                )
            ;
        EndSQL
      end

      def read_job_seq(job_id)
        @connection.query_value(<<-EndSQL)
            select dwh_job_seq
            from dwh_jobs
            where dwh_job_id = #{s job_id}
            ;
        EndSQL
      end

      def task_processed?(task)
        n_nonerror_jobs = @connection.query_value(<<-EndSQL)
            select
                count(*)
            from
                dwh_tasks t
                inner join dwh_jobs j using (dwh_task_seq)
                left outer join dwh_job_results r using (dwh_job_seq)
            where
                t.dwh_task_seq = #{task.task_seq}
                and (
                    r.status is null         -- running
                    or r.status = 'success'  -- finished with success
                )
            ;
        EndSQL
        @logger.debug "n_nonerror_jobs=#{n_nonerror_jobs.inspect}"
        n_nonerror_jobs.to_i > 0
      end

      def table_in_use?(task)
        n_working_jobs = @connection.query_value(<<-EndSQL)
            select
                count(*)
            from
                dwh_tasks t
                inner join dwh_jobs j using (dwh_task_seq)
                left outer join dwh_job_results r using (dwh_job_seq)
            where
                t.schema_name = #{s task.schema}
                and t.table_name = #{s task.table}
                and r.status is null   -- running
            ;
        EndSQL
        @logger.debug "n_working_jobs=#{n_working_jobs.inspect}"
        n_working_jobs.to_i > 0
      end

      def stage_files(task)
        # already-processed object_url MUST be removed
        @connection.execute(<<-EndSQL)
            insert into dwh_str_load_files
                ( dwh_job_seq
                , object_url
                )
            select
                #{@job_seq}
                , object_url
            from
                dwh_str_load_files_incoming
            where
                dwh_task_seq = #{task.task_seq}
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

        staged_files = @connection.query_values(<<-EndSQL)
            select
                object_url
            from
                dwh_str_load_files
            where
                dwh_job_seq = #{@job_seq}
            ;
        EndSQL
        @logger.debug "n_staged_files=#{staged_files.size}"

        staged_files
      end

      def load_objects(dest_table, manifest, options)
        @connection.execute(<<-EndSQL.strip.gsub(/\s+/, ' '))
            copy #{dest_table}
            from '#{manifest.url}'
            credentials '#{manifest.credential_string}'
            manifest
            statupdate false
            compupdate false
            #{options}
            ;
        EndSQL
        @logger.info "load succeeded: #{manifest.url}" unless @noop
      end

      def commit_work_table(task)
        insert_stmt = @sql ? @sql.source : "insert into #{task.dest_table} select * from #{task.work_table};"
        @connection.execute(insert_stmt)
        # keep work table records for later tracking
      end

      def commit_job_result
        @end_time = Time.now
        write_job_result 'success', ''
      end

      MAX_MESSAGE_LENGTH = 1000

      def abort_job(status, message)
        @end_time = Time.now
        write_job_result status, message.lines.first.strip[0, MAX_MESSAGE_LENGTH]
      end

      def write_job_result(status, message)
        @connection.execute(<<-EndSQL)
          insert into dwh_job_results
              ( dwh_job_seq
              , utc_end_time
              , status
              , message
              )
          values
              ( #{@job_seq}
              , #{t @end_time.getutc}
              , #{s status}
              , #{s message}
              )
          ;
        EndSQL
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

      attr_reader :task_seq
      attr_reader :task_id
      attr_reader :schema
      attr_reader :table

      def dest_table
        "#{@schema}.#{@table}"
      end

      def work_table
        "#{@schema}.#{@table}_wk"
      end

      def use_work_table?
        true    # FIXME
      end

    end


    class LoaderConnection

      extend Forwardable

      def LoaderConnection.open(ds, logger:, noop: false)
        ds.open {|conn|
          yield new(ds, conn, logger: logger, noop: noop)
        }
      end

      def initialize(ds, conn, logger:, noop: false)
        @ds = ds
        @connection = conn
        @logger = logger
        @noop = noop
      end

      def_delegator '@connection', :transaction
      def_delegator '@connection', :query
      def_delegator '@connection', :query_row
      def_delegator '@connection', :query_value
      def_delegator '@connection', :query_values

      def execute(query)
        if @noop
          @connection.log_query(query)
        else
          @connection.update(query)
        end
      end

      def lock(table)
        execute("lock #{table}")
      end

    end


    class ManifestFile

      def ManifestFile.create(ds, job_id:, object_urls:, logger:, noop: false)
        manifest = new(ds, job_id, object_urls, noop: noop)
        logger.info "s3: put: #{manifest.url}"
        manifest.put
        yield manifest
#logger.info "s3: delete: #{manifest.url}"
#manifest.delete
      end

      def initialize(ds, job_id, object_urls, noop: false)
        @ds = ds
        @job_id = job_id
        @object_urls = object_urls
        @noop = noop
      end

      def credential_string
        @ds.credential_string
      end

      def name
        @name ||= "manifest-#{@job_id}.json"
      end

      def url
        @url ||= @ds.url(name)
      end

      def content
        @content ||= begin
          ents = @object_urls.map {|url|
            { "url" => url, "mandatory" => true }
          }
          obj = { "entries" => ents }
          JSON.pretty_generate(obj)
        end
      end

      def put
        @ds.object(name).put(body: content) unless @noop
      end

      def delete
        @ds.object(name).delete unless @noop
      end

    end

  end

end
