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
        @row_count = 0
        @success_value = encode_to_hash? ? 1 : "OK"
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

      def encode_to_hash?
        @encode == 'hash'
      end

      def log_write_result(write_futures)
        count = 0
        dup_count = 0
        write_futures.each do |f|
          count, dup_count = count_result(f.value, @success_value, count, dup_count)
        end
        # divide by column_count when hash (which returns row * columns count of results)
        count, dup_count = count_hash_rows(count, dup_count) if encode_to_hash?
        ds.logger.info "keys already exist: #{dup_count}"
        ds.logger.info "keys written: #{count}"
      end

      def count_result(result, success_value, count, dup_count)
        if result == success_value
          count += 1
        else
          dup_count += 1
        end
        return count, dup_count
      end

      def count_hash_rows(count, dup_count)
        return count / @column_count, dup_count / @column_count
      end

      def delete(keys)
        ds.logger.info "Delete: #{key_pattern}"
        ds.client.del(*keys)
      end

      def write
        futures = []
        @src.execute_query(source) do |rs|
          rs.each do |row|
            futures.push(set @encode, prefix+row['id'], row)
            @row_count += 1
          end
          # save number of columns for log_writer_result()
          @column_count = rs.first.size unless @column_count
        end
        futures.flatten
      end

      def set(type, key, row)
        # write only when key does not exist
        fs = []
        case type
        when 'hash'
          # set a value for each key:field pair
          row.each do |field,value|
            fs.push(ds.client.hsetnx key, field, value)
          end
        when 'json'
          fs.push(ds.client.setnx key, JSON.generate(row))
        else
          raise "\"encode: #{type}\" is not supported"
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
        ds.logger.info "Keys read: #{@row_count}"
        log_write_result @futures
        JobResult.success
      end
    end
  end
end
