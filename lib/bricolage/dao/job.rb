module Bricolage
  module DAO
    class Job

      Attributes = Struct.new(:id, :subsystem, :job_name, :jobnet_id)

      def initialize(datasource)
        @datasource = datasource
        @conn = @datasource.open
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
              "subsystem" = '#{subsystem}'
              and "job_name" = '#{job_name}'
              and jobnet_id = #{jobnet_id}
          ;
        SQL

        if job.empty?
          []
        else
          Attributes.new(job['job_id'], job['subsystem'], job['job_name'], job['jobnet_id'])
        end
      end

      def create_if_not_exist(subsystem, job_name, jobnet_id)
        job = @conn.query_row(<<~SQL)
          insert into jobs ("subsystem", "job_name", jobnet_id)
              values ('#{subsystem}', '#{job_name}', #{jobnet_id})
              on conflict ("subsystem", "job_name", jobnet_id) do nothing
              returning "job_id", "subsystem", "job_name", jobnet_id
          ;
        SQL

        Attributes.new(job['job_id'], job['subsystem'], job['job_name'], job['jobnet_id'])
      end

      def find_or_create(subsystem, job_name, jobnet_id)
        find(subsystem, job_name, jobnet_id) || create_if_not_exist(subsystem, job_name, jobnet_id)
      end
    end
  end
end
