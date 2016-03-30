require 'bricolage/exception'
require 'pg'

module Bricolage

  class PostgreSQLException < SQLException; end

  class PostgresConnection
    def initialize(connection, ds, logger)
      @connection = connection
      @ds = ds
      @logger = logger
    end

    def source
      @connection
    end

    def execute_update(query)
      log_query query
      rs = log_elapsed_time {
        @connection.exec(query)
      }
      result = rs.to_a
      rs.clear
      result
    rescue PG::Error => ex
      raise PostgreSQLException.wrap(ex)
    end

    alias execute execute_update
    alias update execute_update

    def select(table, &block)
      execute_query("select * from #{table}", &block)
    end

    def query_value(query)
      row = query_row(query)
      row ? row.values.first : nil
    end

    def query_values(query)
      execute_query(query) {|rs| rs.to_a }.flat_map {|rec| rec.values }
    end

    def query_row(query)
      execute_query(query) {|rs| rs.to_a.first }
    end

    def execute_query(query, &block)
      log_query query
      rs = log_elapsed_time {
        @connection.exec(query)
      }
      result = yield rs
      rs.clear
      result
    rescue PG::Error => ex
      raise PostgreSQLException.wrap(ex)
    end

    alias query execute_query

    def query_batch(query, batch_size = 5000, &block)
      open_cursor(query) {|cur|
        cur.each_result_set(batch_size, &block)
      }
    end

    def streaming_execute_query(query, &block)
      log_query query
      log_elapsed_time {
        @connection.send_query(query)
      }
      @connection.set_single_row_mode
      while rs = @connection.get_result
        begin
          rs.check
          yield rs
        ensure
          rs.clear
        end
      end
    rescue PG::Error => ex
      raise PostgreSQLException.wrap(ex)
    end

    def in_transaction?
      @connection.transaction_status == PG::Constants::PQTRANS_INTRANS
    end

    def transaction
      execute 'begin transaction'
      yield
      execute 'commit'
    end

    def open_cursor(query, name = nil, &block)
      unless in_transaction?
        transaction {
          return open_cursor(query, &block)
        }
      end
      name ||= make_unique_cursor_name
      execute "declare #{name} cursor for #{query}"
      yield Cursor.new(name, self, @logger)
    end

    Thread.current['bricolage_cursor_seq'] = 0

    def make_unique_cursor_name
      seq = (Thread.current['bricolage_cursor_seq'] += 1)
      "cur_bric_#{$$}_#{'%X' % Thread.current.object_id}_#{seq}"
    end

    class Cursor
      def initialize(name, conn, logger)
        @name = name
        @conn = conn
        @logger = logger
      end

      attr_reader :name

      def each_result_set(fetch_size = 5000)
        while true
          @conn.execute_query("fetch #{fetch_size} in #{@name}") {|rs|
            return if rs.values.empty?
            yield rs
          }
        end
      end
    end

    def drop_table(name)
      execute "drop table #{name} cascade;"
    end

    def drop_table_force(name)
      drop_table name
    rescue PostgreSQLException => err
      @logger.error err.message
    end

    def vacuum(table)
      execute "vacuum #{table};"
    end

    def vacuum_sort_only(table)
      execute "vacuum sort only #{table};"
    end

    def analyze(table)
      execute "analyze #{table};"
    end

    def log_query(query)
      @logger.info "[#{@ds.name}] #{mask_secrets query}"
    end

    def mask_secrets(msg)
      msg.gsub(/\bcredentials\s+'.*?'/mi, "credentials '****'")
    end
    private :mask_secrets

    def log_elapsed_time
      b = Time.now
      return yield
    ensure
      e = Time.now
      t = e - b
      @logger.info "#{'%.1f' % t} secs"
    end

  end

end
