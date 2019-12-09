module Bricolage
  module DAO
    class Job

      include SQLUtils

      Attributes = Struct.new(:id, :subsystem, :job_name, :jobnet_id, :executor_id)

      def Job.for_record(job)
        Attributes.new(*job.values)
      end

      def Job.for_records(jobs)
        jobs.map { |job| Job.for_record(job) }
      end

      def initialize(datasource)
        @datasource = datasource
      end

      def find_by(subsystem, job_name, jobnet_id)
        record = @datasource.open_shared_connection do |conn|
          conn.query_row(<<~SQL)
            select
                "job_id"
                , "subsystem"
                , "job_name"
                , jobnet_id
                , "executor_id"
            from
                jobs
            where
                "subsystem" = #{s(subsystem)}
                and "job_name" = #{s(job_name)}
                and jobnet_id = #{jobnet_id}
            ;
          SQL
        end

        if record.nil?
          nil
        else
          Job.for_record(record)
        end
      end

      def create(subsystem, job_name, jobnet_id)
        record = @datasource.open_shared_connection do |conn|
          conn.query_row(<<~SQL)
            insert into jobs ("subsystem", "job_name", jobnet_id)
                values (#{s(subsystem)}, #{s(job_name)}, #{jobnet_id})
                returning "job_id", "subsystem", "job_name", jobnet_id
            ;
          SQL
        end

        Job.for_record(record)
      end

      def find_or_create(subsystem, job_name, jobnet_id)
        find_by(subsystem, job_name, jobnet_id) || create(subsystem, job_name, jobnet_id)
      end

      def where(**args)
        where_clause = compile_where_expr(args)
        records = @datasource.open_shared_connection do |conn|
          conn.query_rows(<<~SQL)
            select
                "job_id"
                , "subsystem"
                , "job_name"
                , jobnet_id
                , "executor_id"
            from
                jobs
            where
                #{where_clause}
            ;
          SQL
        end

        if records.empty?
          []
        else
          Job.for_records(records)
        end
      end

      def update(where:, set:)
        set_columns, set_values = compile_set_expr(set)
        set_clause = set.map{|k,v| "#{k} = #{convert_value(v)}"}.join(', ')

        where_clause = compile_where_expr(where)
        records = @datasource.open_shared_connection do |conn|
          conn.query_rows(<<~SQL)
            update jobs set #{set_clause} where #{where_clause} returning *;
          SQL
        end

        if records.empty?
          []
        else
          Job.for_records(records)
        end
      end

      def check_lock(job_ids)
        records = @datasource.open_shared_connection do |conn|
          conn.query_rows(<<~SQL)
            select job_id from jobs where job_id in (#{job_ids.join(',')}) and executor_id is not null;
          SQL
        end

        records.compact.size > 0
      end
    end

  end
end
