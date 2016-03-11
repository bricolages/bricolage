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
      @opts = opts
    end

    attr_reader :host
    attr_reader :port
    attr_reader :opts

    def new_task
      RedisTask.new(self)
    end

    def client
      @client = Redis.new(:host => @host, :port => @port, **@opts) unless @client
      @client
    end
  end

  class RedisTask < DataSourceTask
    def import(src, table, query, key, prefix, encode, delete: false)
      add Import.new(src, table, query, key, encode, delete)
    end

    class Import < Action
      def initialize(src, table, query, key, encode, delete)
        @src = src
        @table = table
        @query = query
        @key = key
        @encode = encode
        @delete = delete
      end

      def bind(*args)
        @query.bind(*args)
      end

      def source
        @query.stripped_source
      end

      def prefix
        return @prefix unless @prefix.nil?
        @prefix = "#{@table.last.name}_#{@table.last.schema}_"
        ds.logger.info "Key Pattern: #{@prefix}<#{@key}>"
        @prefix
      end

      def key_pattern
        prefix + "*"
      end

      def log_write_result(write_futures)
        count = 0
        write_futures.each do |f|
          # invoke Future#value() to raise exception when error
          # when json
          count += 1 if f.value == "OK"
          # when hash
          count += 1 if f.value == true
        end
        # divide by column_number when hash (which returns row * columns number of results)
        count = count / @column_number if @encode == 'hash'
        ds.logger.info "keys written: #{count}"
      end

      def delete(keys)
        ds.logger.info "Delete: #{key_pattern}"
        ds.client.del(*keys)
      end

      def write
        futures = []
        @src.execute_query(source) do |rs|
          rs.each do |row|
            futures.push(set prefix+row['id'], row)
            # save number of columns for log_writer_result()
            @column_number = row.size unless @column_number
          end
        end
        futures.flatten
      end

      def set(key, row)
        # write only when key does not exist
        fs = []
        if @encode == 'json'
          fs.push(ds.client.setnx key, JSON.generate(row))
        else
          # set a value for each key:field pair
          row.each do |field,value|
            fs.push(ds.client.hsetnx key, field, value)
          end
        end
        fs
      end

      def run
        begin
          keys_to_delete = ds.client.keys(key_pattern)
          ds.client.pipelined do # for bulk processing (futures only available after pipeline finished)
            ds.client.multi do # for transaction
              @delete_future = delete keys_to_delete if @delete
              @futures = write
            end
          end
        rescue => ex
          ds.logger.error ex.backtrace.join("\n")
          raise JobFailure, ex.message
        end
        ds.logger.info "Keys deleted: #{@delete_future.value}" if @delete
        log_write_result @futures
        JobResult.success
      end
    end
  end
end
