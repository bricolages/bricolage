require 'bricolage/streamingload/manifest'
require 'bricolage/sqlutils'
require 'securerandom'
require 'json'

module Bricolage

  module StreamingLoad

    class Loader

      include SQLUtils

      def initialize(ctx, logger:)
        @ctx = ctx
        @logger = logger

        @job_id = SecureRandom.uuid
        @job_seq = nil
        @start_time = Time.now
        @end_time = nil
      end

      def process(task, params)
        params.ds.open {|conn|
          @connection = conn
          assign_task task
          do_load task, params
        }
      end

      def assign_task(task)
        @connection.transaction {
          @connection.lock('dwh_tasks')
          @connection.lock('dwh_jobs')
          @connection.lock('dwh_job_results')

          processed = task_processed?(task)
          table_in_use = table_in_use?(task)
          write_job(task.seq)
          @job_seq = read_job_seq(@job_id)
          @logger.info "dwh_job_seq=#{@job_seq}"
          abort_job 'failure', "task is already processed by other jobs" if processed
          abort_job 'failure', "other jobs are working on the target table" if table_in_use
        }
      end

      def do_load(task, params)
        staged_files = stage_files(task)
        return if staged_files.empty?
        ManifestFile.create(
          params.ctl_bucket,
          job_id: @job_id,
          object_urls: staged_files,
          logger: @logger
        ) {|manifest|
          if params.enable_work_table?
            prepare_work_table params.work_table
            load_objects params.work_table, manifest, params.load_options_string
            @connection.transaction {
              commit_work_table params
              commit_job_result
            }
          else
            @connection.transaction {
              load_objects params.dest_table, manifest, params.load_options_string
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

      def task_processed?(task)
        n_nonerror_jobs = @connection.query_value(<<-EndSQL)
            select
                count(*)
            from
                dwh_tasks t
                inner join dwh_jobs j using (dwh_task_seq)
                left outer join dwh_job_results r using (dwh_job_seq)
            where
                t.dwh_task_seq = #{task.seq}
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
                dwh_task_seq = #{task.seq}
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

      def abort_job(status, message)
        write_job_error(status, message)
        raise JobFailure, message
      end

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
