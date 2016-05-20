require 'bricolage/streamingload/loaderparams'
require 'bricolage/streamingload/manifest'
require 'bricolage/sqlutils'
require 'securerandom'
require 'json'

module Bricolage

  module StreamingLoad

    class Loader

      include SQLUtils

      def Loader.load_from_file(ctx, task, logger:)
        params = LoaderParams.load(ctx, task)
        new(ctx, params, logger: logger)
      end

      def initialize(ctx, params, logger:)
        @ctx = ctx
        @params = params
        @logger = logger

        @job_id = SecureRandom.uuid
        @job_seq = nil
        @start_time = Time.now
        @end_time = nil
      end

      def execute
        @params.ds.open {|conn|
          @connection = conn
          assign_task
          do_load
        }
      end

      def assign_task
        success = true
        @connection.transaction {|txn|
          @connection.lock('dwh_tasks')
          @connection.lock('dwh_jobs')
          @connection.lock('dwh_job_results')

          already_processed = task_processed?(@params.task_seq)
          table_in_use = table_in_use?(@params.schema, @params.table)

          write_job(@params.task_seq)
          @job_seq = read_job_seq(@job_id)
          @logger.info "dwh_job_seq=#{@job_seq}"

          # These result record MUST be written in the same transaction.
          # DO NOT raise exception here
          if already_processed
            write_job_error 'failure', "task is already processed by other jobs"
            success = false
          elsif table_in_use
            write_job_error 'failure', "other jobs are working on the target table"
            success = false
          end
        }
        raise JobFailure, "could not assign task: #{@params.task_id}" unless success
      end

      def do_load
        staged_files = stage_files(@params.task_seq)
        return if staged_files.empty?
        ManifestFile.create(
          @params.ctl_bucket,
          job_id: @job_id,
          object_urls: staged_files,
          logger: @logger
        ) {|manifest|
          if @params.enable_work_table?
            prepare_work_table @params.work_table
            load_objects @params.work_table, manifest, @params.load_options_string
            @connection.transaction {
              commit_work_table @params
              commit_job_result
            }
          else
            @connection.transaction {
              load_objects @params.dest_table, manifest, @params.load_options_string
              commit_job_result
            }
          end
        }
      rescue JobFailure => ex
        write_job_error 'failure', ex.message
        raise
      rescue Exception => ex
        write_job_error 'error', ex.message
        raise
      end

      def write_job(task_seq)
        @connection.execute(<<-EndSQL)
            insert into dwh_jobs
                ( dwh_job_id
                , job_class
                , utc_start_time
                , process_id
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

      def task_processed?(task_seq)
        n_nonerror_jobs = @connection.query_value(<<-EndSQL)
            select
                count(*)
            from
                dwh_tasks t
                inner join dwh_jobs j using (dwh_task_seq)
                left outer join dwh_job_results r using (dwh_job_seq)
            where
                t.dwh_task_seq = #{task_seq}
                and (
                    r.status is null         -- running
                    or r.status = 'success'  -- finished with success
                )
            ;
        EndSQL
        @logger.debug "n_nonerror_jobs=#{n_nonerror_jobs.inspect}"
        n_nonerror_jobs.to_i > 0
      end

      def table_in_use?(schema, table)
        n_working_jobs = @connection.query_value(<<-EndSQL)
            select
                count(*)
            from
                dwh_tasks t
                inner join dwh_jobs j using (dwh_task_seq)
                left outer join dwh_job_results r using (dwh_job_seq)
            where
                t.schema_name = #{s schema}
                and t.table_name = #{s table}
                and r.status is null   -- running
            ;
        EndSQL
        @logger.debug "n_working_jobs=#{n_working_jobs.inspect}"
        n_working_jobs.to_i > 0
      end

      def stage_files(task_seq)
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
                dwh_task_seq = #{task_seq}
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

      def prepare_work_table(work_table)
        @connection.execute("truncate #{work_table}")
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
        @logger.info "load succeeded: #{manifest.url}"
      end

      def commit_work_table(params)
        @connection.execute(params.sql_source)
        # keep work table records for later tracking
      end

      def commit_job_result
        @end_time = Time.now
        write_job_result 'success', ''
      end

      MAX_MESSAGE_LENGTH = 1000

      def write_job_error(status, message)
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

  end

end
