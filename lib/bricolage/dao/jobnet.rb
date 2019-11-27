module Bricolage
  module DAO
    class JobNet

      include SQLUtils

      Attributes = Struct.new(:id, :subsystem, :jobnet_name, :executor_id)

      def JobNet.for_record(jobnet)
        Attributes.new(*jobnet.values)
      end

      def JobNet.for_records(jobnets)
        jobnets.map { |jobnet| JobNet.for_record(jobnet) }
      end

      def initialize(datasource)
        @datasource = datasource
      end

      def find(subsystem, jobnet_name)
        jobnet = @datasource.open_shared_connection do |conn|
          conn.query_row(<<~SQL)
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
        end

        if jobnet.nil?
          nil
        else
          JobNet.for_record(jobnet)
        end
      end

      def create(subsystem, jobnet_name)
        jobnet = @datasource.open_shared_connection do |conn|
          conn.query_row(<<~SQL)
            insert into jobnets ("subsystem", jobnet_name)
                values (#{s(subsystem)}, #{s(jobnet_name)})
                returning jobnet_id, "subsystem", jobnet_name
            ;
          SQL
        end

        JobNet.for_record(jobnet)
      end

      def find_or_create(subsystem, jobnet_name)
        find(subsystem, jobnet_name) || create(subsystem, jobnet_name)
      end
    end
  end
end
