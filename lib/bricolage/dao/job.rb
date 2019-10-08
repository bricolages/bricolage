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
              "subsystem" = '#{escape(subsystem)}'
              and "job_name" = '#{escape(job_name)}'
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
              values ('#{escape(subsystem)}', '#{escape(job_name)}', #{jobnet_id})
              returning "job_id", "subsystem", "job_name", jobnet_id
          ;
        SQL

        Attributes.new(job['job_id'], job['subsystem'], job['job_name'], job['jobnet_id'])
      end

      def find_or_create(subsystem, job_name, jobnet_id)
        find(subsystem, job_name, jobnet_id) || create(subsystem, job_name, jobnet_id)
      end

      private

      def escape(string)
        return string unless string
        string.to_s.gsub(/'/, "''").gsub(/\\/, '\\\\')
      end
    end
  end
end
