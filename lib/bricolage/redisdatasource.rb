require 'bricolage/datasource'
require 'bricolage/commandutils'
require 'redis'
require 'json'

module Bricolage

  class RedisDataSource < DataSource
    declare_type 'redis'

    def initialize(host: 'localhost', port: 6380, **options)
      @host = host
      @port = port
      @options = options
    end

    attr_reader :host
    attr_reader :port
    attr_reader :options

    def new_task
      RedisTask.new(self)
    end

    def open
      client = Redis.new(host: @host, port: @port, **@options)
      yield client
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
        @prefix = prefix || "#{@table.last.schema}_#{@table.last.name}_"
        @encode = encode
        @expire = expire
      end

      def bind(*args)
        @query.bind(*args)
      end

      def source
        @query.stripped_source
      end

      def run
        logger = ds.logger
        begin
          logger.info "Key Pattern: #{@prefix}<#{@key_columns.join('_')}>"
          logger.info "Encode: #{@encode}"
          logger.info "Expire: #{@expire}"
          ds.open {|client|
            writer = RedisRowWriter.for_encode(@encode).new(client, @prefix, @key_columns)
            import writer
          }
        rescue => ex
          logger.exception ex
          raise JobFailure, ex.message
        end
        JobResult.success
      end

      BATCH_SIZE = 5000

      def import(writer)
        count = 0
        @src.query_batch(source, BATCH_SIZE) do |rs|
          writer.pipelined {
            rs.each do |row|
              writer.write(row)
              count += 1
              ds.logger.info "transfered: #{count} rows" if count % 100_0000 == 0
            end
          }
        end
        ds.logger.info "all rows written: #{count} rows"
      end
    end
  end

  class RedisRowWriter
    def RedisRowWriter.for_encode(encode)
      case encode
      when 'hash' then RedisHashRowWriter
      when 'json' then RedisJSONRowWriter
      else
        raise ParameterError, "unsupported Redis encode: #{encode.inspect}"
      end
    end

    def initialize(client, prefix, key_columns)
      @client = client
      @prefix = prefix
      @key_columns = key_columns
    end

    attr_reader :prefix
    attr_reader :write_count

    def key(row)
      @prefix + @key_columns.map {|k| row[k] }.join('_')
    end

    def value_columns(row)
      r = row.dup
      @key_columns.each do |key|
        r.delete(key)
      end
      r.empty? ? {1 => 1} : r
    end

    def pipelined(&block)
      @client.pipelined(&block)
    end

    def write(row)
      key = key(row)
      futures = do_write(key, value_columns(row))
      futures.push @client.expire(key, @expire) if @expire
      futures
    end

    def expire
      @client.expire(key, @expire)
    end
  end

  class RedisHashRowWriter < RedisRowWriter
    def do_write(key, values)
      # set a value for each key:field pair
      values.map {|field, value|
        @client.hset(key, field, value)
      }
    end
  end

  class RedisJSONRowWriter < RedisRowWriter
    def do_write(key, values)
      future = @client.set(key, JSON.generate(values))
      [future]
    end
  end

end
