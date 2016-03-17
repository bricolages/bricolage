require 'bricolage/datasource'
require 'bricolage/commandutils'
require 'redis'
require 'json'

module Bricolage

  class RedisDataSource < DataSource
    declare_type 'redis'

    def initialize(host: 'localhost', port: 6380, **opts)
      @host = host
      @port = port
      @options = opts
    end

    attr_reader :host
    attr_reader :port
    attr_reader :opts

    def new_task
      RedisTask.new(self)
    end

    def client
      @client = @client || Redis.new(:host => @host, :port => @port, **@options)
    end
  end

  class RedisTask < DataSourceTask
    def import(src, table, query, key_column, prefix, encode, expire: nil)
      add Import.new(src, table, query, key_column, prefix, encode, expire)
    end

    class Import < Action
      def initialize(src, table, query, key_column, prefix, encode, expire)
        @src = src
        @table = table
        @query = query
        @key_columns = key_column.split(',').map(&:strip)
        @prefix = prefix
        @encode = encode
        @expire = expire
        @read_count = 0
        @write_count = 0
      end

      def bind(*args)
        @query.bind(*args)
      end

      def source
        @query.stripped_source
      end

      def prefix
        @prefix = @prefix || "#{@table.last.schema}_#{@table.last.name}_"
      end

      def import
        @src.cursor_transaction {
          read_count = 0
          loop do
            ds.client.pipelined do 
              read_count = read_row do |row|
                write_row row
              end
            end
            break if read_count == 0
          end
        }
        @cursor = nil
      end

      def read_row
        rs_size = 0
        @cursor = @src.cursor_execute_query(source, cursor: @cursor) do |rs|
          rs_size = rs.values.size
          break if rs_size == 0
          rs.each do |row|
            yield row
            @read_count += 1
            ds.logger.info "Rows read: #{@read_count}" if @read_count % 100000 == 0
          end
        end
        rs_size
      end

      def write_row(row)
        key = key(row)
        data = delete_key_columns(row)
        f = []
        case @encode
        when 'hash'
          # set a value for each key:field pair
          data.each do |field,value|
            f.push ds.client.hset(key, field, value)
          end
        when 'json'
          f.push ds.client.set(key, JSON.generate(data))
        else
          raise %Q("encode: #{type}" is not supported)
        end
        f.push ds.client.expire(key, @expire) if @expire
        @write_count += 1
        return f
      end

      def delete_key_columns(row)
        r = row.dup
        @key_columns.each do |key|
          r.delete(key)
        end
        r.empty? ? {1 => 1} : r
      end

      def key(row)
        prefix + @key_columns.map {|k| row[k]}.join('_')
      end

      def run
        begin
          ds.logger.info "Key Pattern: #{prefix}<#{@key_columns.join('_')}>"
          ds.logger.info "Encode: #{@encode}"
          ds.logger.info "Expire: #{@expire}"
          import
        rescue => ex
          ds.logger.error ex.backtrace.join("\n")
          raise JobFailure, ex.message
        end
        ds.logger.info "Rows written: #{@write_count}"
        JobResult.success
      end
    end
  end
end
