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

    def execute(query)
      @logger.info "[#{@ds.name}] #{query}"
      log_elapsed_time {
        rs = @connection.exec(query)
        result = rs.to_a
        rs.clear
        result
      }
    rescue PG::Error => ex
      raise PostgreSQLException.wrap(ex)
    end

    def execute_query(query, &block)
      @logger.info "[#{@ds.name}] #{query}"
      exec(query, &block)
    end

    alias update execute

    def drop_table(name)
      execute "drop table #{name} cascade;"
    end

    def drop_table_force(name)
      drop_table name
    rescue PostgreSQLException => err
      @logger.error err.message
    end

    def select(table, &block)
      query = "select * from #{table}"
      @logger.info "[#{@ds.name}] #{query}"
      exec(query, &block)
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

    private

    def log_elapsed_time
      b = Time.now
      return yield
    ensure
      e = Time.now
      t = e - b
      @logger.info "#{'%.1f' % t} secs"
    end
    
    def exec(query, &block)
      @connection.send_query(query)
      @connection.set_single_row_mode
      loop do
        rs = @connection.get_result or break
        begin
          rs.check
          yield rs
        ensure
          rs.clear
        end
      end
    end
  end

end
