module Bricolage
  module DAO
    class Job

      Job = Struct.new(:id, :subsystem, :job_name, :jobnet_id)

      def initialize(datasource)
        @datasource = datasource
      end

      def find(subsystem, job_name, jobnet_id)
        job = @datasource.open do |conn|
          conn.query_row(<<~SQL)
          SELECT * FROM jobs WHERE subsystem = '#{subsystem}' AND job_name = '#{job_name}' AND jobnet_id = #{jobnet_id};
          SQL
        end

        Job.new(job['job_id'], job['subsystem'], job['job_name'], job['jobnet_id']) unless job.empty?
      end

      def create(subsystem, job_name, jobnet_id)
        job = @datasource.open do |conn|
          conn.query_row(<<~SQL)
          INSERT INTO jobs (subsystem, job_name, jobnet_id)
            VALUES ('#{subsystem}', '#{job_name}', #{jobnet_id})
            ON CONFLICT (subsystem, job_name, jobnet_id) DO NOTHING
            RETURNING job_id, subsystem, job_name, jobnet_id
          ;
          SQL
        end

        Job.new(job['job_id'], job['subsystem'], job['job_name'], job['jobnet_id']) unless job.empty?
      end

      def find_or_create(subsystem, job_name, jobnet_id)
        find(subsystem, job_name, jobnet_id) || create(subsystem, job_name, jobnet_id)
      end
    end
  end
end
