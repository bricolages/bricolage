require 'bricolage/exception'
require 'pg'

module Bricolage

  class PostgreSQLException < SQLException; end

  class PostgresConnection

    def PostgresConnection.install_signal_handlers
      Signal.trap(:TERM) {
        $stderr.puts 'receive SIGTERM'
        raise Interrupt, 'SIGTERM'
      }
    end

    def PostgresConnection.open_data_source(ds)
      conn = _open_ds(ds)
      if block_given?
        begin
          yield conn
        ensure
          conn.close_force
        end
      else
        return conn
      end
    end

    def PostgresConnection._open_ds(ds)
      conn = PG::Connection.open(host: ds.host, port: ds.port, dbname: ds.database, user: ds.user, password: ds.password)
      new(conn, ds, ds.logger)
    rescue PG::ConnectionBad, PG::UnableToSend => ex
      raise ConnectionError.wrap(ex)
    end
    private_class_method :_open_ds

    def initialize(connection, ds, logger)
      @connection = connection
      @ds = ds
      @logger = logger
      @closed = false
      @connection_failed = false
    end

    def source
      @connection
    end

    def close
      @connection.close
      @closed = true
    end

    def close_force
      close
    rescue
    end

    def closed?
      @closed
    end

    def cancel_force
      cancel
    rescue PostgreSQLException
    end

    def cancel
      @logger.info "cancelling PostgreSQL query..."
      err = @connection.cancel
      if err
        @logger.error "could not cancel query: #{err}"
        raise PostgreSQLException, "could not cancel query: #{err}"
      else
        @logger.info "successfully cancelled"
      end
    end

    def querying
      return yield
    rescue Interrupt
      @logger.info "query interrupted"
      cancel_force
      raise
    end
    private :querying

    def execute_update(query)
      log_query query
      rs = log_elapsed_time {
        querying {
          @connection.async_exec(query)
        }
      }
      return rs.to_a
    rescue PG::ConnectionBad, PG::UnableToSend => ex
      @connection_failed = true
      raise ConnectionError.wrap(ex)
    rescue PG::Error => ex
      raise PostgreSQLException.wrap(ex)
    ensure
      rs.clear if rs
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
        querying {
          @connection.async_exec(query)
        }
      }
      return (yield rs)
    rescue PG::ConnectionBad, PG::UnableToSend => ex
      @connection_failed = true
      raise ConnectionError.wrap(ex)
    rescue PG::Error => ex
      raise PostgreSQLException.wrap(ex)
    ensure
      rs.clear if rs
    end

    alias query execute_query

    def query_batch(query, batch_size = 5000, &block)
      open_cursor(query) {|cur|
        cur.each_result_set(batch_size, &block)
      }
    end

    def streaming_execute_query(query, &block)
      in_exec = false
      log_query query
      log_elapsed_time {
        in_exec = true
        @connection.send_query(query)
      }
      @connection.set_single_row_mode
      while rs = @connection.get_result
        # NOTE: query processing is in progress until the first result set is returned.
        in_exec = false
        begin
          rs.check
          yield rs
        ensure
          rs.clear
        end
      end
    rescue Interrupt
      cancel_force if in_exec
      raise
    rescue PG::ConnectionBad, PG::UnableToSend => ex
      @connection_failed = true
      raise ConnectionError.wrap(ex)
    rescue PG::Error => ex
      raise PostgreSQLException.wrap(ex)
    end

    def in_transaction?
      @connection.transaction_status == PG::Constants::PQTRANS_INTRANS
    end

    def transaction
      execute 'begin transaction'
      txn = Transaction.new(self)
      begin
        yield txn
      rescue
        begin
          if not txn.committed? and not @connection_failed
            txn.abort
          end
        rescue => ex
          @logger.error "SQL error on transaction abort: #{ex.message} (ignored)"
        end
        raise
      ensure
        txn.commit unless txn.committed?
      end
    end

    class Transaction
      def initialize(conn)
        @conn = conn
        @committed = false
      end

      def committed?
        @committed
      end

      def commit
        @conn.execute 'commit'
        @committed = true
      end

      def abort
        @conn.execute 'abort'
        @committed = true
      end

      def truncate_and_commit(table)
        @conn.execute "truncate #{table}"
        @committed = true
      end
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
      execute "drop table if exists #{name} cascade;"
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

    def lock(table)
      execute("lock #{table}")
    end

    def log_query(query)
      @logger.log(@ds.sql_log_level) { "[#{@ds.name}] #{mask_secrets query}" }
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
      @logger.log(@ds.sql_log_level) { "#{'%.1f' % t} secs" }
    end

  end

end
