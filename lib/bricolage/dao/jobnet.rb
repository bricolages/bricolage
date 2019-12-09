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

      def find_by(subsystem, jobnet_name)
        record = @datasource.open_shared_connection do |conn|
          conn.query_row(<<~SQL)
            select
                jobnet_id
                , "subsystem"
                , jobnet_name
                , executor_id
            from
                jobnets
            where
                "subsystem" = #{s(subsystem)}
                and jobnet_name = #{s(jobnet_name)}
            ;
          SQL
        end

        if record.nil?
          nil
        else
          JobNet.for_record(record)
        end
      end

      def create(subsystem, jobnet_name)
        record = @datasource.open_shared_connection do |conn|
          conn.query_row(<<~SQL)
            insert into jobnets ("subsystem", jobnet_name)
                values (#{s(subsystem)}, #{s(jobnet_name)})
                returning jobnet_id, "subsystem", jobnet_name
            ;
          SQL
        end

        JobNet.for_record(record)
      end

      def find_or_create(subsystem, jobnet_name)
        find_by(subsystem, jobnet_name) || create(subsystem, jobnet_name)
      end

      def where(**args)
        where_clause = compile_where_expr(args)

        records = @datasource.open_shared_connection do |conn|
          conn.query_rows(<<~SQL)
            select
                *
            from
                jobnets
            where
                #{where_clause}
            ;
          SQL
        end

        if records.empty?
          []
        else
          JobNet.for_records(records)
        end
      end

      def update(where:, set:)
        set_columns, set_values = compile_set_expr(set)
        set_clause = set.map{|k,v| "#{k} = #{convert_value(v)}"}.join(', ')

        where_clause = compile_where_expr(where)

        records = @datasource.open_shared_connection do |conn|
          conn.query_rows(<<~SQL)
            update jobnets set #{set_clause} where #{where_clause} returning *;
          SQL
        end

        if records.empty?
          []
        else
          JobNet.for_records(records)
        end
      end

      def check_lock(jobnet_id)
        record = @datasource.open_shared_connection do |conn|
          conn.query_row(<<~SQL)
            select jobnet_id from jobnets where jobnet_id = #{jobnet_id} and executor_id is not null;
          SQL
        end

        record != nil
      end

    end
  end
end
