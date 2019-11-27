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

      def find(subsystem, job_name, jobnet_id)
        job = @datasource.open_shared_connection do |conn|
          conn.query_row(<<~SQL)
            select
                "job_id"
                , "subsystem"
                , "job_name"
                , jobnet_id
            from
                jobs
            where
                "subsystem" = #{s(subsystem)}
                and "job_name" = #{s(job_name)}
                and jobnet_id = #{jobnet_id}
            ;
          SQL
        end

        if job.nil?
          nil
        else
          Job.for_record(job)
        end
      end

      def create(subsystem, job_name, jobnet_id)
        job = @datasource.open_shared_connection do |conn|
          conn.query_row(<<~SQL)
            insert into jobs ("subsystem", "job_name", jobnet_id)
                values (#{s(subsystem)}, #{s(job_name)}, #{jobnet_id})
                returning "job_id", "subsystem", "job_name", jobnet_id
            ;
          SQL
        end

        Job.for_record(job)
      end

      def find_or_create(subsystem, job_name, jobnet_id)
        find(subsystem, job_name, jobnet_id) || create(subsystem, job_name, jobnet_id)
      end
    end
  end
end
