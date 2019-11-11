module Bricolage
  module DAO
    class JobNet

      include SQLUtils

      Attributes = Struct.new(:id, :subsystem, :jobnet_name)

      def initialize(datasource)
        @datasource = datasource
        @conn = @datasource.open_shared_connection
      end

      def find(subsystem, jobnet_name)
        jobnet = @conn.query_row(<<~SQL)
          select
              jobnet_id
              , "subsystem"
              , jobnet_name
          from
              jobnets
          where
              "subsystem" = #{s(subsystem)}
              and jobnet_name = #{s(jobnet_name)}
          ;
        SQL

        if jobnet.nil?
          nil
        else
          Attributes.new(jobnet['jobnet_id'], jobnet['subsystem'], jobnet['jobnet_name'])
        end
      end

      def create(subsystem, jobnet_name)
        jobnet = @conn.query_row(<<~SQL)
          insert into jobnets ("subsystem", jobnet_name)
              values (#{s(subsystem)}, #{s(jobnet_name)})
              returning jobnet_id, "subsystem", jobnet_name
          ;
        SQL

        Attributes.new(jobnet['jobnet_id'], jobnet['subsystem'], jobnet['jobnet_name'])
      end

      def find_or_create(subsystem, jobnet_name)
        find(subsystem, jobnet_name) || create(subsystem, jobnet_name)
      end
    end
  end
end
