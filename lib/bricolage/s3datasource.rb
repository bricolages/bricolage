require 'bricolage/datasource'
require 'bricolage/commandutils'
require 'aws-sdk'
require 'stringio'

module Bricolage

  class S3DataSource < DataSource
    declare_type 's3'

    def initialize(
        endpoint: 's3-ap-northeast-1.amazonaws.com',
        region: 'ap-northeast-1',
        bucket: nil,
        prefix: nil,
        access_key_id: nil,
        secret_access_key: nil,
        iam_role: nil,
        master_symmetric_key: nil,
        encryption: nil,
        s3cfg: nil
    )
      @endpoint = (/\Ahttps?:/ =~ endpoint) ? endpoint : "https://#{endpoint}"
      @region = region
      @bucket_name = bucket
      @prefix = (prefix && prefix.empty?) ? nil : prefix
      @access_key_id = access_key_id
      @secret_access_key = secret_access_key
      @iam_role = iam_role
      @master_symmetric_key = master_symmetric_key
      @encryption = encryption
      @s3cfg = s3cfg
      @configurations = @s3cfg ? load_configurations(@s3cfg) : nil
    end

    attr_reader :endpoint
    attr_reader :region
    attr_reader :bucket_name
    attr_reader :prefix

    def new_task
      S3Task.new(self)
    end

    # For Redshift
    def credential_string
      if @iam_role
        "aws_iam_role=#{@iam_role}"
      else
        [
          "aws_access_key_id=#{access_key}",
          "aws_secret_access_key=#{secret_key}",
          (@master_symmetric_key && "master_symmetric_key=#{@master_symmetric_key}")
        ].compact.join(';')
      end
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

    attr_reader :encryption

    def encrypted?
      !!(@master_symmetric_key or @encryption)
    end

    #
    # Ruby Interface
    #

    def client
      @client ||= Aws::S3::Client.new(region: @region, endpoint: @endpoint, access_key_id: access_key, secret_access_key: secret_key)
    end

    def bucket
      @resource ||= Aws::S3::Resource.new(client: client)
      @bucket ||= @resource.bucket(@bucket_name)
    end

    def object(rel, no_prefix: false)
      bucket.object(path(rel, no_prefix: no_prefix))
    end

    def url(rel, no_prefix: false)
      "s3://#{@bucket_name}/#{path(rel, no_prefix: no_prefix)}"
    end

    def path(rel, no_prefix: false)
      path = (no_prefix || !@prefix) ? rel.to_s : "#{@prefix}/#{rel}"
      path.sub(%r<\A/>, '').gsub(%r<//>, '/')
    end

    def traverse(rel, no_prefix: false)
      bucket.objects(prefix: path(rel, no_prefix: no_prefix))
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
          ds.object(dest).upload_file(src)
        end
        nil
      end
    end
  end

end
