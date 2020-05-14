module Bricolage
  module DAO

    class JobExecution
      include  SQLUtils

      STATUS_WAIT    = 'waiting'.freeze
      STATUS_SUCCESS = 'succeeded'.freeze
      STATUS_RUN     = 'running'.freeze
      STATUS_FAILURE = 'failed'.freeze
      STATUS_CANCEL = 'canceled'.freeze

      Attributes = Struct.new(:jobnet_id, :job_id, :job_execution_id, :execution_sequence,
                              :status, :message, :submitted_at, :started_at, :finished_at,
                              :source, :job_name, :jobnet_name, :executor_id, :subsystem,
                              keyword_init: true)

      def JobExecution.for_record(job_execution)
        je_sym_hash = Hash[ job_execution.map{ |k,v| [k.to_sym, v] } ]
        Attributes.new(**je_sym_hash)
      end

      def JobExecution.for_records(job_executions)
        job_executions.map { |je| JobExecution.for_record(je) }
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

      def create(job_id, execution_sequence, status)
        records = @datasource.open_shared_connection {|conn|
          conn.execute_update(<<~SQL)
            insert into job_executions ("job_id", execution_sequence, status)
                values (#{job_id}, #{execution_sequence}, #{s(status)})
                returning job_execution_id, execution_sequence, status, message, job_id, source,
                          submitted_at, started_at, finished_at
            ;
          SQL
        }

        job_execution = JobExecution.for_record(records.first)
        JobExecutionState.job_executions_change(@datasource, [job_execution])
        job_execution
      end

      def where(**args)
        where_clause = compile_where_expr(args)

        records = @datasource.open_shared_connection do |conn|
          conn.query_rows(<<~SQL)
            select
                job_execution_id
                , jn.jobnet_id
                , jn.jobnet_name
                , j.job_id
                , j.job_name
                , j.executor_id as executor_id
                , j.subsystem as subsystem
                , execution_sequence
                , status
                , message
                , source
                , submitted_at
                , started_at
                , finished_at
            from
                job_executions je
                join jobs j using(job_id)
                join jobnets jn using(jobnet_id)
            where
                #{where_clause}
            ;
          SQL
        end

        if records.empty?
          []
        else
          JobExecution.for_records(records)
        end
      end

      def cancel_jobnet(jobnet_ref, message)
        records = connect {|conn|
          conn.execute_update(<<~SQL)
            update job_executions
            set
                status = #{s STATUS_CANCEL}
                , message = #{s message}
                , started_at = now()
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
                and status in (#{s STATUS_WAIT}, #{s STATUS_RUN}, #{s STATUS_FAILURE})
            returning job_execution_id, job_id
            ;
          SQL
        }
        records.map {|r| JobExecution.for_record(r) }
      end

      def update(where:, set:)
        set_columns, set_values = compile_set_expr(set)
        set_clause = set.map{|k,v| "#{k} = #{convert_value(v)}"}.join(', ')

        where_clause = compile_where_expr(where)
        records = @datasource.open_shared_connection do |conn|
          conn.execute_update(<<~SQL)
            update job_executions je
            set #{set_clause}
            from
                jobs j
                join jobnets jn using(jobnet_id)
            where
                je.job_id = j.job_id
                and #{where_clause}
            returning job_execution_id, jn.jobnet_id, jn.jobnet_name, j.job_id, j.job_name,
                      j.executor_id as executor_id, j.subsystem as subsystem, execution_sequence,
                      status, message, source, submitted_at, started_at, finished_at
            ;
          SQL
        end

        job_executions = JobExecution.for_records(records)
        JobExecutionState.job_executions_change(@datasource, job_executions)
        job_executions
      end

      def upsert(set:)
        set_columns, set_values = compile_set_expr(set)
        set_clause = set.map{|k,v| "#{k} = #{convert_value(v)}"}.join(', ')

        records = @datasource.open_shared_connection do |conn|
          conn.query_rows(<<~SQL)
            insert into job_executions (#{set_columns})
                values (#{set_values})
                on conflict (job_id)
                do update set #{set_clause}
            ;
          SQL
        end

        job_executions = JobExecution.for_records(records)
        JobExecutionState.job_executions_change(@datasource, job_executions)
        job_executions
      end

      def delete(**args)
        where_clause = compile_where_expr(args)
        @datasource.open_shared_connection do |conn|
          conn.query_rows(<<~SQL)
            begin;
              delete from job_execution_states as jes
                using job_executions as je
                where
                  jes.job_execution_id = je.job_execution_id
                  and #{where_clause}
              ;
              delete from job_executions as je where #{where_clause};
            commit;
          SQL
        end
      end
    end

    class JobExecutionState
      include SQLUtils

      def self.job_executions_change(datasource, job_executions)
        state = new(datasource)
        job_executions.each do |je|
          state.create(
            job_execution_id: je.job_execution_id,
            status: je.status || '',
            message: je.message || '',
            job_id: je.job_id
          )
        end
      end

      def JobExecutionState.for_connection(conn)
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

      def create(job_execution_id:, status:, message:, job_id:)
        columns = 'job_execution_id, status, message, created_at, job_id'
        values = "#{job_execution_id}, #{s(status)}, #{s(message)}, now(), #{job_id}"
        @datasource.open_shared_connection do |conn|
          conn.execute("insert into job_execution_states (#{columns}) values (#{values});")
        end
      end

      def cancel_jobs(job_executions, message)
        connect {|conn|
          job_executions.each do |job_exec|
            conn.execute(<<~EndSQL)
              insert into job_execution_states
              ( job_execution_id
              , status
              , message
              , created_at
              , job_id
              )
              values
              ( #{job_exec.job_execution_id}
              , #{s JobExecution::STATUS_CANCEL}
              , #{s message}
              , now()
              , #{job_exec.job_id}
              )
              ;
            EndSQL
          end
        }
      end
    end
  end
end
