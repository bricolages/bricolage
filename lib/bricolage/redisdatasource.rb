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
    def import(src, table, query, key_column, prefix, encode)
      add Import.new(src, table, query, key_column, prefix, encode)
    end

    class Import < Action
      def initialize(src, table, query, key_column, prefix, encode)
        @src = src
        @table = table
        @query = query
        @key_column = key_column
        puts key_column
        @prefix = prefix
        @encode = encode
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
        read_row do |row|
          write_row(row)
        end
      end

      def read_row
        @src.execute_query(source) do |rs|
          rs.each do |row|
            yield row
            @read_count += 1
            ds.logger.info "Rows read: #{@read_count}" if @read_count % 100000 == 0
          end
        end
      end

      def write_row(row, &block)
        key = key(row)
        case @encode
        when 'hash'
          # set a value for each key:field pair
          r = []
          row.each do |field,value|
            r.push ds.client.hset(key, field, value)
          end
        when 'json'
          r = ds.client.set(key, JSON.generate(row))
        else
          raise %Q("encode: #{type}" is not supported)
        end
        yield r if block
        ds.logger.info "Key sample: #{key}" if @write_count == 0
        @write_count += 1
      end

      def key(row)
        key_columns = @key_column.split(',').map(&:strip).map {|k| row[k]}
        prefix + key_columns.join('_')
      end

      def run
        begin
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
