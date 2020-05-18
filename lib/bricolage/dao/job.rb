require 'bricolage/exception'

module Bricolage
  module DAO

    class Job
      include SQLUtils

      Attributes = Struct.new(:id, :subsystem, :job_name, :jobnet_id, :executor_id, keyword_init: true)

      def Job.for_record(r)
        Attributes.new(
          id: r['job_id']&.to_i,
          subsystem: r['subsystem'],
          job_name: r['job_name'],
          jobnet_id: r['jobnet_id']&.to_i,
          executor_id: r['executor_id']
        )
      end

      def initialize(datasource)
        @datasource = datasource
      end

      private def connect(&block)
        @datasource.open_shared_connection(&block)
      end

      def find_or_create(jobnet_id, job_ref)
        connect {|conn|
          job = find(conn, jobnet_id, job_ref)   # optimize the most frequent case
          if job
            job
          else
            begin
              create(conn, jobnet_id, job_ref)
            rescue UniqueViolationException
              find(conn, jobnet_id, job_ref) or raise "[BUG] Could not find/create job record: jobnet_id=#{jobnet_id}, ref=#{job_ref}"
            end
          end
        }
      end

      private def find(conn, jobnet_id, job_ref)
        record = conn.query_row(<<~EndSQL)
            select
                "job_id"
                , "subsystem"
                , "job_name"
                , "executor_id"
                , jobnet_id
            from
                jobs
            where
                jobnet_id = #{jobnet_id}
                and "subsystem" = #{s job_ref.subsystem}
                and "job_name" = #{s job_ref.name}
            ;
        EndSQL

        if record
          Job.for_record(record)
        else
          nil
        end
      end

      private def create(conn, jobnet_id, job_ref)
        record = conn.query_row(<<~EndSQL)
            insert into jobs
                ( "subsystem"
                , "job_name"
                , jobnet_id
                )
            values
                ( #{s job_ref.subsystem}
                , #{s job_ref.name}
                , #{jobnet_id}
                )
            returning "job_id", "subsystem", "job_name", jobnet_id
            ;
        EndSQL

        Job.for_record(record)
      end

      def locked?(job_ids)
        count = connect {|conn|
          conn.query_value(<<~EndSQL)
              select
                  count(job_id)
              from
                  jobs
              where
                  job_id in (#{job_ids.join(',')})
                  and executor_id is not null
              ;
          EndSQL
        }

        count.to_i > 0
      end

      def locked_jobs(jobnet_id)
        records = connect {|conn|
          conn.query_rows(<<~EndSQL)
              select
                  "job_id"
                  , "subsystem"
                  , "job_name"
                  , jobnet_id
                  , "executor_id"
              from
                  jobs
              where
                  jobnet_id = #{jobnet_id}
                  and executor_id is not null
              ;
          EndSQL
        }

        if records.empty?
          []
        else
          record.map {|r| Job.for_record(r) }
        end
      end

      def lock(job_id, executor_id)
        records = connect {|conn|
          conn.execute_update(<<~EndSQL)
              update jobs
              set
                  executor_id = #{s executor_id}
              where
                  job_id = #{job_id}
                  and executor_id is null
              returning job_id
              ;
          EndSQL
        }

        if records.empty?
          raise DoubleLockError, "Could not lock job: job_id=#{job_id}"
        end
      end

      # Unlock the job.
      # Returns true if successfully unlocked, otherwise false.
      # FIXME: raise an exception on failing unlock?
      def unlock(job_id, executor_id)
        records = connect {|conn|
          conn.execute_update(<<~EndSQL)
            update jobs
            set
                executor_id = null
            where
                job_id = #{job_id}
                and executor_id = #{s executor_id}
            returning job_id
            ;
          EndSQL
        }

        not records.empty?
      end

      def clear_lock_all(jobnet_id)
        connect {|conn|
          conn.execute_update(<<~EndSQL)
            update jobs
            set
                executor_id = null
            where
                jobnet_id = #{jobnet_id}
            ;
          EndSQL
        }
      end

    end   # class Job

  end
end
