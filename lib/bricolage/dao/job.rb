module Bricolage
  module DAO
    class Job

      include SQLUtils

      Attributes = Struct.new(:id, :subsystem, :job_name, :jobnet_id)

      def initialize(datasource)
        @datasource = datasource
        @conn = @datasource.open_shared_connection
      end

      def find(subsystem, job_name, jobnet_id)
        job = @conn.query_row(<<~SQL)
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

        if job.nil?
          nil
        else
          Attributes.new(job['job_id'], job['subsystem'], job['job_name'], job['jobnet_id'])
        end
      end

      def create(subsystem, job_name, jobnet_id)
        job = @conn.query_row(<<~SQL)
          insert into jobs ("subsystem", "job_name", jobnet_id)
              values (#{s(subsystem)}, #{s(job_name)}, #{jobnet_id})
              returning "job_id", "subsystem", "job_name", jobnet_id
          ;
        SQL

        Attributes.new(job['job_id'], job['subsystem'], job['job_name'], job['jobnet_id'])
      end

      def find_or_create(subsystem, job_name, jobnet_id)
        find(subsystem, job_name, jobnet_id) || create(subsystem, job_name, jobnet_id)
      end
    end
  end
end
