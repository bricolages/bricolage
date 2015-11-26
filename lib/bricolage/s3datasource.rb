require 'bricolage/datasource'
require 'bricolage/commandutils'
require 'aws-sdk'
require 'stringio'

module Bricolage

  class S3DataSource < DataSource
    declare_type 's3'

    def initialize(endpoint: 's3-ap-northeast-1.amazonaws.com',
        bucket: nil, prefix: nil,
        access_key_id: nil, secret_access_key: nil, master_symmetric_key: nil,
        s3cfg: nil)
      @endpoint = endpoint
      @bucket = bucket
      @prefix = (prefix && prefix.empty?) ? nil : prefix
      @access_key_id = access_key_id
      @secret_access_key = secret_access_key
      @master_symmetric_key = master_symmetric_key
      @s3cfg = s3cfg
      @configurations = @s3cfg ? load_configurations(@s3cfg) : nil
    end

    attr_reader :endpoint
    attr_reader :bucket
    attr_reader :prefix

    def new_task
      S3Task.new(self)
    end

    # For Redshift
    def credential_string
      [
        "aws_access_key_id=#{access_key}",
        "aws_secret_access_key=#{secret_key}",
        (@master_symmetric_key && "master_symmetric_key=#{@master_symmetric_key}")
      ].compact.join(';')
    end

    def access_key
      @access_key_id || get_config('access_key')
    end

    def secret_key
      @secret_access_key || get_config('secret_key')
    end

    def get_config(key)
      @configurations[key] or raise ParameterError, "missing s3cfg entry: #{key}"
    end

    def load_configurations(path)
      h = {}
      File.foreach(path) do |line|
        case line
        when /\A\s*\w+\s*=\s*/
          key, value = line.split('=', 2)
          val = value.strip
          h[key.strip] = val.empty? ? nil : val
        end
      end
      h
    end

    def encrypted?
      !!@master_symmetric_key
    end

    #
    # Ruby Interface
    #

    def client
      @client ||= AWS::S3.new(s3_endpoint: endpoint, access_key_id: access_key, secret_access_key: secret_key)
    end

    def objects
      client.buckets[bucket].objects
    end

    def objects_with_prefix(rel, no_prefix: false)
      objects.with_prefix(path(rel, no_prefix: no_prefix))
    end

    def object(rel, no_prefix: false)
      objects[path(rel, no_prefix: no_prefix)]
    end

    def url(rel, no_prefix: false)
      "s3://#{@bucket}/#{path(rel, no_prefix: no_prefix)}"
    end

    def path(rel, no_prefix: false)
      path = (no_prefix || !prefix) ? rel.to_s : "#{@prefix}/#{rel}"
      path.sub(%r<\A/>, '').gsub(%r<//>, '/')
    end
  end

  class S3Task < DataSourceTask
    def put(src, dest, check_args: true)
      add Put.new(src, dest).tap {|action| action.check_arguments if check_args }
    end

    class Put < Action
      def initialize(src, dest)
        @src = src
        @dest = dest
      end

      def source_files
        @source_files ||= Dir.glob(@src)
      end

      def single_source?
        source_files.size == 1 and source_files.first == @src
      end

      def each_src_dest
        source_files.each do |src|
          dest = (@dest.to_s.end_with?('/') ? "#{@dest}/#{File.basename(src)}" : @dest)
          yield src, dest
        end
      end

      def command_line(src, dest)
        "aws s3 cp #{src} #{ds.url(dest)}"
      end

      def check_arguments
      end

      def source
        buf = StringIO.new
        each_src_dest do |src, dest|
          buf.puts command_line(src, dest)
        end
        buf.string
      end

      def run
        raise JobFailure, "no such file: #{@src}" if source_files.empty?
        each_src_dest do |src, dest|
          ds.logger.info command_line(src, dest)
          ds.object(dest).write(file: src)
        end
        nil
      end
    end
  end

end
