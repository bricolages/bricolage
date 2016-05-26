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
          # Do nothing if the task already running or succeeded
          # Retry if error
          return if task_processed?(@params.task_seq)
          assign_task
          #FIXME unprocessed job remains if loader dies this here, before finishing load
          #You can retry it by inserting result as error if the task message is still in sqs
          do_load
        }
      end

      def assign_task
        @connection.transaction {|txn|
          @job_seq = write_job(@params.task_seq)
          @logger.info "dwh_job_seq=#{@job_seq}"
          stage_files(@params.task_seq)
        }
      end

      def do_load
        files = staged_files
        if files.empty?
          @connection.transaction {
            commit_job_result
          }
        end
        ManifestFile.create(
          @params.ctl_bucket,
          job_id: @job_id,
          object_urls: files,
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
        job_seq = @connection.query_value(<<-EndSQL)
            select dwh_job_seq
            from dwh_jobs
            where dwh_job_id = #{s @job_id}
            ;
        EndSQL
        job_seq
      end

      def task_processed?(task_seq)
        n_nonerror_jobs = @connection.query_value(<<-EndSQL)
            select
                count(distinct dwh_job_seq)
            from
                dwh_jobs t
            left outer join
                dwh_job_results
                using(dwh_job_seq)
            where
                t.dwh_task_seq = #{task_seq}
                and (
                    status is null -- runnnig
                    or status = 'success'
                )
            ;
        EndSQL
        @logger.debug "n_nonerror_jobs=#{n_nonerror_jobs.inspect}"
        n_nonerror_jobs.to_i > 0
      end

      def stage_files(task_seq)
        # already-processed object_url MUST be removed
        # retry errored objects
        @connection.execute(<<-EndSQL)
            insert into dwh_str_load_files
                ( dwh_job_seq
                , object_url
                )
            select
                #{@job_seq}
                , object_url
            from (
                select
                    object_url
                from
                    dwh_str_load_files_incoming
                where
                    dwh_task_seq = #{task_seq}
                except
                select
                    object_url
                from
                    dwh_str_load_files
                left outer join
                    dwh_job_results
                using(dwh_job_seq)
                where status is null -- runnnig
                or status = 'success' -- success
                )
            ;
        EndSQL
      end

      def staged_files
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
