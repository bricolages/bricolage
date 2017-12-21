require 'bricolage/datasource'
require 'mysql2'
require 'json'
require 'csv'
require 'stringio'
require 'open3'

module Bricolage

  class MySQLDataSource < DataSource
    declare_type 'mysql'

    def initialize(**mysql_options)
      @mysql_options = mysql_options
      @client = nil
    end

    attr_reader :mysql_options

    def host
      @mysql_options[:host]
    end

    def port
      @mysql_options[:port]
    end

    def username
      @mysql_options[:username]
    end

    def password
      @mysql_options[:password]
    end

    def database
      @mysql_options[:database]
    end

    def new_task
      MySQLTask.new(self)
    end

    def open
      @client = Mysql2::Client.new(**@mysql_options)
      begin
        yield self
      ensure
        c = @client
        @client = nil
        c.close
      end
    end

    def query(sql, **opts)
      logger.info "[SQL] #{sql}"
      connection_check
      @client.query(sql, **opts)
    end

    private

    def connection_check
      unless @client
        raise FatalError, "#{self.class} used outside of \#open block"
      end
    end
  end

  class MySQLTask < DataSourceTask
    def export(stmt, path: nil, format: nil, override: false, gzip: false, sqldump: false)
      add Export.new(stmt, path: path, format: format, override: override, gzip: gzip, sqldump: sqldump)
    end

    class Export < Action
      def initialize(stmt, path: nil, format: nil, override: false, gzip: false, sqldump: false)
        @statement = stmt
        @path = path
        @format = format
        @override = override
        @gzip = gzip
        @sqldump = sqldump
      end

      def bind(*args)
        @statement.bind(*args)
      end

      def source
        @statement.stripped_source
      end

      def run
        if @sqldump and sqldump_available? and sqldump_usable?
          export_by_sqldump
        else
          export_by_ruby
        end
        JobResult.success
      end

      def export_by_sqldump
        cmds = [[{"SQLDUMP_PASSWORD" => ds.password}, sqldump_path.to_s, "--#{@format}", ds.host, ds.port.to_s, ds.username, ds.database, @statement.stripped_source]]
        cmds.push [GZIP_COMMAND] if @gzip
        cmds.last.push({out: @path.to_s})
        ds.logger.info '[CMD] ' + format_pipeline(cmds)
        statuses = Open3.pipeline(*cmds)
        statuses.each_with_index do |st, idx|
          unless st.success?
            cmd = cmds[idx].first
            raise JobFailure, "sqldump failed (status #{st.to_i})"
          end
        end
      end

      def format_pipeline(cmds)
        cmds = cmds.map {|args| args[0].kind_of?(Hash) ? args[1..-1] : args.dup }   # do not show env
        cmds.map {|args| %Q("#{args.join('" "')}") }.join(' | ')
      end

      def sqldump_available?
        sqldump_real_path.executable?
      end

      def sqldump_path
        Pathname(__dir__).parent.parent + "libexec/sqldump"
      end

      def sqldump_real_path
        Pathname("#{sqldump_path}.#{platform_name}")
      end

      def platform_name
        @platform_name ||= `uname -s`.strip
      end

      def sqldump_usable?
        %w[json tsv].include?(@format)
      end

      def export_by_ruby
        ds.logger.info "exporting table into #{@path} ..."
        count = 0
        open_target_file(@path) {|f|
          writer_class = WRITER_CLASSES[@format] or raise ArgumentError, "unknown export format: #{@format.inspect}"
          writer = writer_class.new(f)
          rs = ds.query(@statement.stripped_source, as: writer_class.record_format, stream: true, cache_rows: false)
          ds.logger.info "got result set, writing..."
          rs.each do |values|
            writer.write_record values
            count += 1
            ds.logger.info "#{count} records exported..." if count % 10_0000 == 0
          end
        }
        ds.logger.info "#{count} records exported; export finished"
      end

      private

      # FIXME: parameterize
      GZIP_COMMAND = 'gzip'

      def open_target_file(path, &block)
        unless @override
          raise JobFailure, "destination file already exists: #{path}" if File.exist?(path)
        end
        if @gzip
          ds.logger.info "enable compression: gzip"
          IO.popen(%Q(#{GZIP_COMMAND} > "#{path}"), 'w', &block)
        else
          File.open(path, 'w', &block)
        end
      end
    end

    def s3export(table, stmt, s3ds, prefix, gzip, dump_options)
      options = dump_options.nil? ? {} : dump_options[:dump_options]
      add S3Export.new(table, stmt, s3ds, prefix, gzip: gzip,
        format: options['format'],
        partition_column: options['partition_column'],
        partition_number: options['partition_number'],
        write_concurrency: options['write_concurrency'],
        rotation_size: options['rotation_size'],
        delete_objects: options['delete_objects'],
        object_key_delimiter: options['object_key_delimiter'],
        src_zone_offset: options['src_zone_offset'],
        dst_zone_offset: options['dst_zone_offset'])
    end

    class S3Export < Action

      def initialize(table, stmt, s3ds, prefix, gzip: true,
         format: "json",
         partition_column: nil,
         partition_number: 4,
         write_concurrency: 4,
         rotation_size: nil,
         delete_objects: false,
         object_key_delimiter: nil,
         src_zone_offset: nil,
         dst_zone_offset: nil)
        @table = table
        @statement = stmt
        @s3ds = s3ds
        @prefix = build_prefix @s3ds.prefix, prefix
        @format = format
        @gzip = gzip
        @partition_column = partition_column
        @partition_number = partition_number
        @write_concurrency = write_concurrency
        @rotation_size = rotation_size
        @delete_objects = delete_objects
        @object_key_delimiter = object_key_delimiter
        @src_zone_offset = src_zone_offset
        @dst_zone_offset = dst_zone_offset
      end

      def run
        s3export
        JobResult.success
      end

      def bind(*args)
        @statement.bind(*args) if @statement
      end

      def source
        "-- myexport #{@table} -> #{@s3ds.bucket_name}/#{@prefix}" +
          (@statement ? "\n#{@statement.stripped_source}" : "")
      end

      def s3export
        cmd = build_cmd(command_parameters)
        ds.logger.info "[CMD] #{cmd}"
        out, st = Open3.capture2e(environment_variables, cmd)
        ds.logger.info "[CMDOUT] #{out}"
        unless st.success?
          msg = extract_exception_message(out)
          raise JobFailure, "mys3dump failed (status: #{st.to_i}): #{msg}"
        end
      end

      def environment_variables
        {
          'AWS_ACCESS_KEY_ID' => @s3ds.access_key,
          'AWS_SECRET_ACCESS_KEY' => @s3ds.secret_key,
          'MYS3DUMP_PASSWORD' => ds.password
        }
      end

      def command_parameters
        params = {
          jar: mys3dump_path.to_s,
          h: ds.host,
          P: ds.port.to_s,
          D: ds.database,
          u: ds.username,
          #p: ds.password,
          o: connection_property,
          t: @table,
          b: @s3ds.bucket.name,
          x: @prefix
        }
        params[:q] = @statement.stripped_source.chomp(';') if @statement
        params[:f] = @format if @format
        params[:C] = nil if @gzip
        params[:c] = @partition_column if @partition_column
        params[:n] = @partition_number if @partition_number
        params[:w] = @write_concurrency if @write_concurrency
        params[:r] = @rotation_size if @rotation_size
        params[:d] = nil if @delete_objects
        params[:k] = @object_key_delimiter if @object_key_delimiter
        if src_zone_offset = @src_zone_offset || ds.mysql_options[:src_zone_offset]
          params[:S] = src_zone_offset
        end
        if dst_zone_offset = @dst_zone_offset || ds.mysql_options[:dst_zone_offset]
          params[:T] = dst_zone_offset
        end
        params
      end

      OPTION_MAP = {
        encoding: 'useUnicode=true&characterEncoding',
        read_timeout: 'netTimeoutForStreamingResults',
        connect_timeout: 'connectTimeout',
        reconnect: 'autoReconnect',
        collation: 'connectionCollation'
      }

      def connection_property
        ds.mysql_options.map {|k, v| opt = OPTION_MAP[k] ; opt ? "#{opt}=#{v}" : nil }.compact.join('&')
      end

      def build_prefix(ds_prefix, pm_prefix)
        ((ds_prefix || "") + "//" +  (pm_prefix.to_s || "")).gsub(%r<\A/>, '').gsub(%r<//>, '/')
      end

      def mys3dump_path
        Pathname(__dir__).parent.parent + "libexec/mys3dump.jar"
      end

      def build_cmd(options)
        (['java'] + options.flat_map {|k, v| v ? ["-#{k}", v.to_s] : ["-#{k}"] }.map {|o| %Q("#{o}") }).join(" ")
      end

      def extract_exception_message(out)
        out.lines do |line|
          if /^.*Exception: (?<msg>.*)$/ =~ line
            return msg
          end
        end
      end
    end

    WRITER_CLASSES = {}

    class JSONWriter
      def JSONWriter.record_format
        :hash
      end

      def initialize(f)
        @f = f
      end

      def write_record(values)
        @f.puts JSON.dump(values)
      end
    end
    WRITER_CLASSES['json'] = JSONWriter

    class TSVWriter
      def TSVWriter.record_format
        :array
      end

      def initialize(f)
        @f = f
      end

      def write_record(values)
        @f.puts values.join("\t")
      end
    end
    WRITER_CLASSES['tsv'] = TSVWriter

    class CSVWriter
      def CSVWriter.record_format
        :array
      end

      def initialize(f)
        @csv = CSV.new(f)
      end

      def write_record(values)
        @csv.add_row values
      end
    end
    WRITER_CLASSES['csv'] = CSVWriter
  end

end
