module Bricolage
  module DAO
    class JobNet

      include SQLUtils

      Attributes = Struct.new(:id, :subsystem, :jobnet_name, keyword_init: true)

      def JobNet.for_record(r)
        Attributes.new(
          id: r['jobnet_id']&.to_i,
          subsystem: r['subsystem'],
          jobnet_name: r['jobnet_name']
        )
      end

      def JobNet.for_records(jobnets)
        jobnets.map {|jobnet| JobNet.for_record(jobnet) }
      end

      def initialize(datasource)
        @datasource = datasource
      end

      private def connect(&block)
        @datasource.open_shared_connection(&block)
      end

      def find_or_create(ref)
        connect {|conn|
          jobnet = find(conn, ref)
          if jobnet
            return jobnet
          else
            begin
              return create(conn, ref)
            rescue UniqueViolationException
              jobnet = find(conn, ref) or raise "[BUG] Could not create jobnet record: #{ref}"
              return jobnet
            end
          end
        }
      end

      private def create(conn, ref)
        records = conn.execute_update(<<~SQL)
          insert into jobnets
              ( "subsystem"
              , jobnet_name
              )
          values
              ( #{s ref.subsystem}
              , #{s ref.name}
              )
          returning jobnet_id
          ;
        SQL

        Attributes.new(
          id: records.first['jobnet_id']&.to_i,
          subsystem: ref.subsystem,
          jobnet_name: ref.name
        )
      end

      private def find(conn, ref)
        record = conn.query_row(<<~EndSQL)
          select
              jobnet_id
              , "subsystem"
              , jobnet_name
          from
              jobnets
          where
              "subsystem" = #{s ref.subsystem}
              and jobnet_name = #{s ref.name}
          ;
        EndSQL

        if record
          JobNet.for_record(record)
        else
          nil
        end
      end

      def locked?(ref)
        value = connect {|conn|
          conn.query_value(<<~EndSQL)
            select
                count(*)
            from
                jobnets
            where
                "subsystem" = #{s ref.subsystem}
                and jobnet_name = #{s ref.name}
                and executor_id is not null
            ;
          EndSQL
        }

        value.to_i > 0
      end

      def lock(jobnet_id, executor_id)
        records = connect {|conn|
          conn.execute_update(<<~EndSQL)
            update jobnets
            set
                executor_id = #{s executor_id}
            where
                jobnet_id = #{jobnet_id}
                and executor_id is null
            returning jobnet_id
            ;
          EndSQL
        }
        if records.empty?
          raise DoubleLockError, "Could not lock jobnet: jobnet_id=#{jobnet_id}"
        end
      end

      # Unlock jobnet lock.
      # Returns true if unlocked successfully, otherwise false.
      # FIXME: raise exception?
      def unlock(jobnet_id, executor_id)
        records = connect {|conn|
          conn.execute_update(<<~EndSQL)
            update jobnets
            set
                executor_id = null
            where
                jobnet_id = #{jobnet_id}
                and executor_id = #{s executor_id}
            returning jobnet_id
            ;
          EndSQL
        }

        not records.empty?
      end

      def clear_lock(jobnet_id)
        records = connect {|conn|
          conn.execute_update(<<~EndSQL)
            update jobnets
            set
                executor_id = null
            where
                jobnet_id = #{jobnet_id}
            ;
          EndSQL
        }
      end

      def delete(jobnet_id)
        connect {|conn|
          conn.execute_update(<<~EndSQL)
              delete from jobnets
              where jobnet_id = #{jobnet_id}
              ;
          EndSQL
        }
      end

    end
  end
end
