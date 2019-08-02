module Bricolage
  module DAO
    class JobExecution

      JobExecution = Struct.new(:jobnet_id, :subsystem, :job_id, :job_execution_id,
                                :status, :message, :submitted_at, :lock,
                                :started_at, :finished_at, :source, :job_name, :jobnet_name,
                                keyword_init: true)

      def initialize(datasource)
        @datasource = datasource
      end

      def find_by(**args)
        where_clause = DAO.parse_where(args)

        job_executions = @datasource.open do |conn|
          conn.query_rows(<<~SQL)
          SELECT
            *
          FROM
            job_executions je
            JOIN jobs j using(job_id)
            JOIN jobnets jn using(jobnet_id, subsystem)
          WHERE #{where_clause}
          ;
          SQL
        end

        JobExecution.to_struct(job_executions)
      end

      def update(where:, set:)
        set_clause = set.reject{|k,v| v.nil?}.map{|k,v| "#{k} = #{DAO.convert_value(v)}"}.join(', ')
        where_clause = DAO.parse_where(where)

        job_executions = @datasource.open do |conn|
          conn.query_rows(<<~SQL)
          UPDATE
              job_executions
          SET
              #{set_clause}
          FROM
              job_executions je
              JOIN jobs j using(job_id)
              JOIN jobnets jn using(jobnet_id, subsystem)
          WHERE
              #{where_clause}
          RETURNING *
          ;
          SQL
        end
        job_executions = JobExecution.to_struct(job_executions)

        JobExecutionState.job_executions_change(@datasource, job_executions)

        job_executions
      end

      def upsert(where:, set:)
        set_columns, set_values = DAO.parse_set(set)
        where_clause = DAO.parse_where(where)

        job_executions = @datasource.open do |conn|
          conn.query_rows(<<~SQL)
          INSERT INTO
              job_executions (#{set_columns})
          SELECT
              #{set_values}
          FROM
              jobs
          WHERE
              #{where_clause}
          ON CONFLICT  DO NOTHING
          RETURNING *
          ;
          SQL
        end

        job_executions = JobExecution.to_struct(job_executions)

        JobExecutionState.job_executions_change(@datasource, job_executions)

        job_executions
      end

      def JobExecution.to_struct(job_executions)
        if job_executions.empty?
          []
        else
          job_executions.map do |je|
            je_sym_hash = Hash[ je.map{|k,v| [k.to_sym, v] } ]
            JobExecution.new(**je_sym_hash)
          end
        end
      end

    end


    class JobExecutionState
      def self.job_executions_change(datasource, job_executions)
        state = new(datasource)
        job_executions.each do |je|
          state.create(
            job_execution_id: je.job_execution_id,
            status: je.status,
            message: je.message,
            job_id: je.job_id
          )
        end
      end


      def initialize(datasource)
        @datasource = datasource
      end

      def create(job_execution_id:, status:, message:, job_id:)
        columns = 'job_execution_id, status, message, created_at, job_id'
        values = "#{job_execution_id}, '#{status}', '#{message}', NOW(), #{job_id}"
        @datasource.open do |conn|
          conn.execute("INSERT INTO job_execution_states (#{columns}) VALUES (#{values});")
        end
      end
    end


    private

    def self.parse_set(values_hash)
      columns = values_hash.keys.map(&:to_s).join(', ')
      values = values_hash.values.map{|v| convert_value(v) }.join(', ')
      return columns, values
    end

    def self.convert_value(value)
      if value == :now
        'NOW()'
      elsif value == true or value == false
        "#{value}"
      else
        "'#{value}'"
      end
    end

    def self.parse_where(conds_hash)
      conds_hash.map{|k,v| self.convet_cond(k,v) }.join(' AND ')
    end

    def self.convet_cond(column, cond)
      if cond.nil?
        "#{column} IS NULL"
      elsif cond.instance_of?(Array)
        in_clause = cond.map {|c| "'#{c}'"}.join(', ')
        "#{column} IN (#{in_clause})"
      else
        "#{column} = '#{cond}'"
      end
    end


  end
end
