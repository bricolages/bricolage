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
        cmds = [[sqldump_path.to_s, "--#{@format}", ds.host, ds.port.to_s, ds.username, ds.password, ds.database, @statement.stripped_source]]
        cmds.push [GZIP_COMMAND] if @gzip
        cmds.last.push({out: @path.to_s})
        ds.logger.info '[CMD] ' + format_pipeline(cmds)
        statuses = Open3.pipeline(*cmds)
        statuses.each_with_index do |st, idx|
          unless st.success?
            cmd = cmds[idx].first
            raise JobFailure, "#{cmd} failed (status #{st.to_i})"
          end
        end
      end

      def format_pipeline(cmds)
        cmds = cmds.map {|args| args.dup }
        cmds.first[5] = '****'
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
