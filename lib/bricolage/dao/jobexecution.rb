module Bricolage
  module DAO

    class JobExecution
      include SQLUtils

      STATUS_WAITING = 'waiting'.freeze
      STATUS_SUCCEEDED = 'succeeded'.freeze
      STATUS_RUNNING = 'running'.freeze
      STATUS_FAILED = 'failed'.freeze
      STATUS_CANCELED = 'canceled'.freeze

      Attributes = Struct.new(:job_id, :job_execution_id, :subsystem, :job_name, keyword_init: true)

      def JobExecution.for_record(r)
        Attributes.new(
          job_id: r['job_id']&.to_i,
          job_execution_id: r['job_execution_id']&.to_i,
          subsystem: r['subsystem'],
          job_name: r['job_name']
        )
      end

      def JobExecution.for_connection(conn)
        new(nil, connection: conn)
      end

      def initialize(datasource, connection: nil)
        @datasource = datasource
        @connection = connection
      end

      private def connect
        if @connection
          yield @connection
        else
          @datasource.open_shared_connection {|conn|
            yield conn
          }
        end
      end

      def enqueued_jobs(jobnet_ref)
        records = connect {|conn|
          conn.query_rows(<<~EndSQL)
            select
                e.job_execution_id
                , e.job_id
                , j.subsystem
                , j.job_name
            from
                job_executions e
                inner join jobs j using (job_id)
                inner join jobnets n using (jobnet_id)
            where
                n.subsystem = #{s jobnet_ref.subsystem}
                and n.jobnet_name = #{s jobnet_ref.name}
                and e.status in (#{s STATUS_WAITING}, #{s STATUS_RUNNING}, #{s STATUS_FAILED})
            order by
                e.execution_sequence
            ;
          EndSQL
        }
        records.map {|r| JobExecution.for_record(r) }
      end

      def enqueue_job(job, execution_sequence)
        record = nil
        connect {|conn|
          records = conn.execute_update(<<~EndSQL)
            insert into job_executions
                ( job_id
                , execution_sequence
                , status
                , message
                , submitted_at
                )
            values
                ( #{job.id}
                , #{execution_sequence}
                , #{s STATUS_WAITING}
                , ''
                , now()
                )
            returning job_execution_id, job_id
            ;
          EndSQL

          record = records.first
          save_state_transition(conn, record['job_execution_id'], 'submitted_at')
        }

        exec = JobExecution.for_record(record)
        exec.subsystem = job.subsystem
        exec.job_name = job.job_name
        exec
      end

      def cancel_jobnet(jobnet_ref, message)
        connect {|conn|
          records = conn.execute_update(<<~EndSQL)
            update job_executions
            set
                status = #{s STATUS_CANCELED}
                , message = #{s message}
                , finished_at = now()
            where
                job_id in (
                    select
                        j.job_id
                    from
                        jobs j inner join jobnets n using (jobnet_id)
                    where
                        n.subsystem = #{s jobnet_ref.subsystem}
                        and n.jobnet_name = #{s jobnet_ref.name}
                )
                and status in (#{s STATUS_WAITING}, #{s STATUS_RUNNING}, #{s STATUS_FAILED})
            returning job_execution_id
            ;
          EndSQL

          job_execution_ids = records.map {|r| r['job_execution_id'].to_i }
          unless job_execution_ids.empty?
            conn.execute_update(<<~EndSQL)
                insert into job_execution_states
                    ( job_execution_id
                    , job_id
                    , created_at
                    , status
                    , message
                    )
                select
                    job_execution_id
                    , job_id
                    , finished_at
                    , status
                    , message
                from
                    job_executions
                where
                    job_execution_id in (#{job_execution_ids.join(', ')})
                ;
            EndSQL
          end
        }
      end

      def transition_to_running(job_execution_id)
        connect {|conn|
          records = conn.execute_update(<<~EndSQL)
              update job_executions
              set
                  status = #{s STATUS_RUNNING}
                  , message = ''
                  , started_at = now()
                  , finished_at = null
              where
                  job_execution_id = #{job_execution_id}
                  and status in (#{s STATUS_WAITING}, #{s STATUS_FAILED})
              returning job_execution_id
              ;
          EndSQL
          if records.empty?
            raise IllegalJobStateException, "Could not run already running job: job_execution_id=#{job_execution_id}"
          end

          save_state_transition(conn, job_execution_id, 'started_at')
        }
      end

      def transition_to_succeeded(job_execution_id)
        connect {|conn|
          records = conn.execute_update(<<~EndSQL)
              update job_executions
              set
                  finished_at = now()
                  , status = #{s STATUS_SUCCEEDED}
                  , message = ''
              where
                  job_execution_id = #{job_execution_id}
                  and status = #{s STATUS_RUNNING}
              returning job_execution_id
              ;
          EndSQL
          if records.empty?
            raise IllegalJobStateException, "could not transition to succeeded state: job_execution_id=#{job_execution_id}"
          end

          save_state_transition(conn, job_execution_id, 'finished_at')
        }
      end

      def transition_to_failed(job_execution_id, message)
        connect {|conn|
          records = conn.execute_update(<<~EndSQL)
              update job_executions
              set
                  finished_at = now()
                  , status = #{s STATUS_FAILED}
                  , message = #{s message}
              where
                  job_execution_id = #{job_execution_id}
                  and status = #{s STATUS_RUNNING}
              returning job_execution_id
              ;
          EndSQL
          if records.empty?
            raise IllegalJobStateException, "could not transition to failed state: job_execution_id=#{job_execution_id}"
          end

          save_state_transition(conn, job_execution_id, 'finished_at')
        }
      end

      private def save_state_transition(conn, job_execution_id, time_expr)
        conn.execute_update(<<~EndSQL)
            insert into job_execution_states
                ( job_execution_id
                , job_id
                , created_at
                , status
                , message
                )
            select
                job_execution_id
                , job_id
                , #{time_expr}
                , status
                , message
            from
                job_executions
            where
                job_execution_id = #{job_execution_id}
            ;
        EndSQL
      end

      def delete_all(job_execution_ids)
        connect {|conn|
          conn.execute_update(<<~EndSQL)
              delete from job_execution_states
              where job_execution_id in #{job_execution_ids.join(', ')}
              ;

              delete from job_executions
              where job_execution_id in #{job_execution_ids.join(', ')}
              ;
          EndSQL
        }
      end

    end   # class JobExecution

  end
end
