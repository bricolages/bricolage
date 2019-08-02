module Bricolage
  module DAO
    class JobNet

      JobNet = Struct.new(:id, :subsystem, :jobnet_name)

      def initialize(datasource)
        @datasource = datasource
      end

      def find(subsystem, jobnet_name)
        jobnet = @datasource.open do |conn|
          conn.query_row(<<~SQL)
          SELECT *  FROM jobnets WHERE subsystem = '#{subsystem}' AND jobnet_name = '#{jobnet_name}';
          SQL
        end

        JobNet.new(jobnet['jobnet_id'], jobnet['subsystem'], jobnet['jobnet_name']) unless jobnet.empty?
      end

      def create(subsystem, jobnet_name)
        jobnet = @datasource.open do |conn|
          conn.query_row(<<~SQL)
          INSERT INTO jobnets (subsystem, jobnet_name)
            VALUES ('#{subsystem}', '#{jobnet_name}')
            ON CONFLICT (subsystem, jobnet_name) DO NOTHING
            RETURNING jobnet_id, subsystem, jobnet_name
          ;
          SQL
        end

        JobNet.new(jobnet['jobnet_id'], jobnet['subsystem'], jobnet['jobnet_name']) unless jobnet.empty?
      end

      def find_or_create(subsystem, jobnet_name)
        find(subsystem, jobnet_name) || create(subsystem, jobnet_name)
      end
    end
  end
end
