require 'bricolage/exception'
require 'securerandom'
require 'pg'

module Bricolage

  class PostgreSQLException < SQLException; end

  class PostgresConnection
    def initialize(connection, ds, logger)
      @connection = connection
      @ds = ds
      @logger = logger
      @cursor = nil
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

    def execute_query_with_cursor(query, fetch_size, cursor, &block)
      raise "Begin transaction before invoking this method" unless in_transaction?
      if @cursor.nil?
        @cursor = cursor || (0...32).map { alphabets[rand(alphabets.length)] }.join
        declare_cursor = "declare #{@cursor} cursor for #{query}"
        @logger.info "[#{@ds.name}] #{declare_cursor}"
        @connection.exec(declare_cursor)
      elsif !@cursor.nil? && cursor.nil?
        raise "Cannot declare new cursor. Cursor in use: #{@cursor}"
      elsif @cursor != cursor
        raise "Specified cursor not exists. Specified: #{cursor}, Current: #{@cursor}"
      end
      fetch = "fetch #{fetch_size} in #{@cursor}"
      @logger.info "[#{@ds.name}] #{fetch}" if cursor.nil?
      yield @connection.exec(fetch)
      return @cursor
    end

    def clear_cursor
      @cursor = nil
    end

    def alphabets
      @alphabets = @alphabets || [('a'...'z'), ('A'...'Z')].map { |i| i.to_a }.flatten
    end

    def in_transaction?
      @connection.transaction_status == PG::Constants::PQTRANS_INTRANS
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
