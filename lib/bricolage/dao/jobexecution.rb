module Bricolage
  module DAO
    class JobExecution

      Attributes = Struct.new(:jobnet_id, :subsystem, :job_id, :job_execution_id,
                                :status, :message, :submitted_at, :lock,
                                :started_at, :finished_at, :source, :job_name, :jobnet_name,
                                keyword_init: true)

      def initialize(datasource)
        @datasource = datasource
        @conn = @datasource.open
      end

      def find_by(**args)
        where_clause = DAO.compile_where_expr(args)

        job_executions = @conn.query_rows(<<~SQL)
          select
              *
          from
              job_executions je
              join jobs j using(job_id)
              join jobnets jn using(jobnet_id, "subsystem")
          where
              #{where_clause}
          ;
        SQL

        if job_executions.empty?
          []
        else
          JobExecution.for_record(job_executions)
        end
      end

      def update(where:, set:)
        set_columns, set_values = DAO.compile_set_expr(set)
        set_clause = set.map{|k,v| "#{k} = #{DAO.convert_value(v)}"}.join(', ')

        where_clause = DAO.compile_where_expr(where)

        job_executions = @conn.query_rows(<<~SQL)
          update job_executions je
          set #{set_clause}
          from
              jobs j
              join jobnets jn using(jobnet_id, "subsystem")
          where
              je.job_id = j.job_id
              and #{where_clause}
          ;
        SQL

        job_executions = JobExecution.for_record(job_executions)
        JobExecutionState.job_executions_change(@datasource, job_executions)

        job_executions
      end

      def upsert(set:)
        set_columns, set_values = DAO.compile_set_expr(set)
        set_clause = set.map{|k,v| "#{k} = #{DAO.convert_value(v)}"}.join(', ')

        job_executions = @conn.query_rows(<<~SQL)
          insert into job_executions (#{set_columns})
              values (#{set_values})
              on conflict (job_id)
              do update set #{set_clause}
          ;
        SQL

        job_executions = JobExecution.for_record(job_executions)
        JobExecutionState.job_executions_change(@datasource, job_executions)

        job_executions
      end

      def JobExecution.for_record(job_executions)
        job_executions.map do |je|
          je_sym_hash = Hash[ je.map{|k,v| [k.to_sym, v] } ]
          Attributes.new(**je_sym_hash)
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
        @conn = @datasource.open
      end

      def create(job_execution_id:, status:, message:, job_id:)
        columns = 'job_execution_id, status, message, created_at, job_id'
        values = "#{job_execution_id}, '#{status}', '#{message}', now(), #{job_id}"
        @conn.execute("insert into job_execution_states (#{columns}) values (#{values});")
      end
    end

    private

    def self.compile_set_expr(values_hash)
      columns = values_hash.keys.map(&:to_s).join(', ')
      values = values_hash.values.map{|v| convert_value(v) }.join(', ')
      return columns, values
    end

    def self.convert_value(value)
      if value == :now
        'now()'
      elsif value.nil?
        "null"
      elsif value == true or value == false
        "#{value.to_s}"
      elsif value.instance_of?(Integer) or value.instance_of?(Float)
        "#{value.to_s}"
      elsif value.instance_of?(String) or value.instance_of?(Pathname)
        "'#{value}'"
      else
        raise "invalid type for 'value' argument in JobExecution.convert_value: #{value} is #{value.class}"
      end
    end

    def self.compile_where_expr(conds_hash)
      conds_hash.map{|k,v| self.convert_cond(k,v) }.join(' and ')
    end

    def self.convert_cond(column, cond)
      if cond.nil?
        "#{column} is null"
      elsif cond.instance_of?(Array) # not support subquery
        in_clause = cond.map{|c| self.convert_cond(column, c)}.join(' or ')
        "(#{in_clause})"
      elsif cond == true or cond == false
        "#{column} is #{cond.to_s}"
      elsif cond.instance_of?(Integer) or cond.instance_of?(Float)
        "#{column} = #{cond}"
      elsif cond.instance_of?(String) or cond.instance_of?(Pathname)
        "#{column} = '#{cond}'"
      else
        raise "invalid type for 'cond' argument in JobExecution.convert_cond: #{cond} is #{cond.class}"
      end
    end
  end
end
