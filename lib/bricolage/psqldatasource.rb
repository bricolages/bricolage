require 'bricolage/datasource'
require 'bricolage/s3datasource'
require 'bricolage/sqlstatement'
require 'bricolage/commandutils'
require 'bricolage/postgresconnection'
require 'bricolage/vacuumlock'
require 'bricolage/exception'
require 'pathname'

module Bricolage

  class PSQLDataSource < DataSource
    declare_type 'psql'

    include CommandUtils

    def initialize(
        host: 'localhost',
        port: 5439,
        database: 'dev',
        username: ENV['LOGNAME'],
        password: nil,
        pgpass: nil,
        encoding: nil,
        psql: 'psql',
        tmpdir: Dir.tmpdir)
      @host = host
      @port = port
      @database = database
      @user = username
      @password = password
      @pgpass = pgpass
      @encoding = encoding
      @psql = psql
      @tmpdir = tmpdir
      raise ParameterError, "missing psql host" unless @host
      raise ParameterError, "missing psql port" unless @port
      raise ParameterError, "missing psql database" unless @database
      raise ParameterError, "missing psql username" unless @user
      unless @pgpass or @password
        raise ParameterError, "missing psql password"
      end
    end

    attr_reader :host
    attr_reader :port
    attr_reader :database
    attr_reader :user

    def new_task
      PSQLTask.new(self)
    end

    def open_for_batch
      # do not call #open
      yield
    end

    def execute(source, options = [])
      make_tmpfile(source, tmpdir: @tmpdir) {|path|
        st = command @psql, "--no-psqlrc", "--host=#{@host}", "--port=#{@port}",
            "--username=#{@user}", @database,
            '--echo-all',
            '-v', 'ON_ERROR_STOP=true',
            '-f', path,
            '--no-password',
            *options,
            env: get_psql_env
        msg = retrieve_last_match_from_stderr(/^psql:.*?:\d+: ERROR: (.*)/, 1) unless st.success?
        JobResult.for_process_status(st, msg)
      }
    end

    def get_psql_env
      env = {}
      if @pgpass
        env["PGPASSFILE"] = @pgpass
      elsif @password
        env["PGPASSWORD"] = @password
      end
      env
    end

    #
    # Ruby Library Interface
    #

    def password
      # FIXME: same user must not exist
      @password ||= read_password_from_pgpass(@pgpass, @user)
    end

    def read_password_from_pgpass(path, user)
      File.read(path).slice(/:#{user}:([^:\r\n]+)$/, 1) or
          raise ParameterError, "could not read password: #{path}, #{user}"
    end

    def open(&block)
      conn = PG::Connection.open(host: @host, port: @port, dbname: @database, user: @user, password: password)
      yield PostgresConnection.new(conn, self, logger)
    ensure
      conn.close if conn
    end

    def query(query)
      open {|conn| conn.query(query) }
    end

    def drop_table(name)
      open {|conn| conn.drop_table(name) }
    end

    def drop_table_force(name)
      open {|conn| conn.drop_table_force(name) }
    end

    def select(table, &block)
      open {|conn| conn.select(table, &block) }
    end

    include VacuumLock

    def vacuum(table)
      serialize_vacuum {
        open {|conn| conn.vacuum(table) }
      }
    end

    def vacuum_sort_only(table)
      serialize_vacuum {
        open {|conn| conn.vacuum_sort_only(table) }
      }
    end

    def analyze(table)
      open {|conn| conn.analyze(table) }
    end
  end

  class DataSource   # reopen
    def redshift_loader_source?
      false
    end
  end

  class S3DataSource   # reopen
    def redshift_loader_source?
      true
    end
  end

  # We don't support dynamodb still now

  class PSQLTask < DataSourceTask
    def exec(stmt)
      add SQLAction.new(stmt)
    end

    def each_statement
      each_action do |action|
        yield action.statement
      end
    end

    class SQLAction < Action
      def initialize(stmt)
        @statement = stmt
      end

      attr_reader :statement

      def_delegator '@statement', :bind
    end

    # override
    def source
      buf = StringIO.new
      buf.puts '\timing on'
      each_statement do |stmt|
        buf.puts
        buf.puts "-- #{stmt.location}" if stmt.location
        buf.puts stmt.stripped_source
      end
      buf.string
    end

    # override
    def run
      VacuumLock.using {
        @ds.execute source
      }
    end

    def run_explain
      @ds.execute explain_source
    end

    def explain_source
      buf = StringIO.new
      each_statement do |stmt|
        buf.puts
        buf.puts "-- #{stmt.location}" if stmt.location
        if support_explain?(stmt.kind)
          buf.puts "explain #{stmt.stripped_source}"
        else
          buf.puts "/* #{stmt.stripped_source} */"
        end
      end
      buf.string
    end

    def support_explain?(statement_kind)
      case statement_kind
      when 'select', 'insert', 'update', 'delete' then true
      else false
      end
    end

    def create_dummy_table(target)
      exec SQLStatement.for_string(
        "\\set ON_ERROR_STOP false\n" +
        "create table #{target} (x int);\n" +
        "\\set ON_ERROR_STOP true\n"
      )
    end

    def drop(target_table)
      exec SQLStatement.for_string("drop table #{target_table} cascade;")
    end

    def drop_if(enabled)
      drop '${dest_table}' if enabled
    end

    def drop_obj_force(type, name)
      exec SQLStatement.for_string(
        "\\set ON_ERROR_STOP false\n" +
        "drop #{type} #{name} cascade;\n" +
        "\\set ON_ERROR_STOP true\n"
      )
    end

    def drop_force(target_table)
      drop_obj_force('table', target_table)
    end

    def drop_view_force(target_view)
      drop_obj_force('view', target_view)
    end

    def drop_force_if(enabled)
      drop_force('${dest_table}') if enabled
    end

    def drop_view_force_if(enabled)
      drop_view_force('${dest_table}') if enabled
    end

    def truncate_if(enabled, target = '${dest_table}')
      exec SQLStatement.for_string("truncate #{target};") if enabled
    end

    def rename_table(src, dest)
      exec SQLStatement.for_string("alter table #{src} rename to #{dest};")
    end

    def vacuum_if(enable_vacuum, enable_vacuum_sort, target = '${dest_table}')
      if enable_vacuum
        serialize_vacuum {
          exec SQLStatement.for_string("vacuum #{target};")
        }
      elsif enable_vacuum_sort
        serialize_vacuum {
          exec SQLStatement.for_string("vacuum sort only #{target};")
        }
      end
    end

    include VacuumLock

    def serialize_vacuum   # override
      exec SQLStatement.for_string psql_serialize_vacuum_begin
      yield
      exec SQLStatement.for_string psql_serialize_vacuum_end
    end

    def analyze_if(enabled, target = '${dest_table}')
      exec SQLStatement.for_string("analyze #{target};") if enabled
    end

    def grant(privilege:, on:, to:)
      exec SQLStatement.for_string("grant #{privilege} on #{on} to #{to};")
    end

    GRANT_OPTS = %w[privilege to]

    def grant_if(opts, target)
      return unless opts
      return if opts.empty?
      unknown_keys = opts.keys - GRANT_OPTS
      raise ParameterError, "unknown grant options: #{unknown_keys.inspect}" unless unknown_keys.empty?
      missing_keys = GRANT_OPTS - opts.keys
      raise ParameterError, %Q(missing grant options: #{missing_keys.inspect}) unless missing_keys.empty?
      args = {on: target}
      opts.each do |k, v|
        args[k.intern] = v
      end
      grant(**args)
    end

    def transaction
      exec SQLStatement.for_string('begin transaction;')
      yield
      exec SQLStatement.for_string('commit;')
    end

    def load(src_ds, src_path, dest_table, format, jsonpath, opts)
      exec SQLStatement.for_string(copy_statement(src_ds, src_path, dest_table, format, jsonpath, opts))
    end

    def copy_statement(src_ds, src_path, dest_table, format, jsonpath, opts)
      unless src_ds.redshift_loader_source?
        raise ParameterError, "input data source does not support redshift as bulk loading source: #{src_ds.name}"
      end
      provide_default_load_options opts, src_ds
      buf = StringIO.new
      buf.puts "copy #{dest_table}"
      buf.puts "from '#{src_ds.url(src_path)}'"
      buf.puts "credentials '#{src_ds.credential_string}'"
      buf.puts format_option(format, src_ds, jsonpath)
      opts.each do |opt|
        buf.puts opt.to_s
      end
      buf.puts ';'
      buf.string
    end

    def provide_default_load_options(opts, src_ds)
      if src_ds.encrypted? and not opts.key?('encrypted')
        opts['encrypted'] = true
      end
    end

    def format_option(fmt, src_ds, jsonpath)
      case fmt
      when 'tsv'
        %q(delimiter '\t')
      when 'csv'
        %q(delimiter ',')
      when 'json'
        jsonpath ? "json \'#{src_ds.url(jsonpath)}\'" : %q(json 'auto')
      else
        raise ParameterError, "unsupported format: #{fmt}"
      end
    end

    def unload(stmt, dest_ds, dest_path, format, opts)
      exec unload_statement(stmt, dest_ds, dest_path, format, opts)
    end

    def unload_statement(stmt, dest_ds, dest_path, format, opts)
      buf = StringIO.new
      buf.puts "unload ('#{format_query(stmt.stripped_raw_content)}')"
      buf.puts "to '#{dest_ds.url(dest_path)}'"
      buf.puts "credentials '#{dest_ds.credential_string}'"
      buf.puts unload_format_option(format, dest_ds)
      opts.each do |opt|
        buf.puts opt.to_s
      end
      buf.puts ';'
      res = StringResource.new(buf.string, stmt.location)
      SQLStatement.new(res, stmt.declarations)
    end

    def unload_format_option(format, ds)
      case format
      when 'tsv'
        %q(delimiter '\t')
      when 'csv'
        %q(delimiter ',')
      else
        raise ParameterError, "unsupported format: #{fmt}"
      end
    end

    def format_query(query)
      query.gsub(/^--.*/, '').strip.gsub(/[ \t]*\n[ \t]*/, ' ').gsub("'", "\\\\'")
    end
  end

  class PSQLLoadOptions
    class << self
      def parse(opts)
        case opts
        when Hash then filter_values(opts)
        when String then parse_string(opts)   # FIXME: remove
        else raise ParameterError, "unsupported value type for load options: #{opts.class}"
        end
      end

      private

      def filter_values(opts)
        list = []
        opts.each do |key, value|
          case value
          when String, Integer, true, false, nil
            list.push Option.new(key.to_s, value)
          else
            raise ParameterError, ""
          end
        end
        new(list)
      end

      def parse_string(str)
        return new unless str
        list = []
        str.split(',').each do |opt_pair|
          opt, value = opt_pair.strip.split('=', 2)
          list.push Option.new(opt, parse_value(value, opt))
        end
        new(list)
      end

      def parse_value(value, opt)
        case value
        when nil, 'true', true then true
        when 'false', false then false
        when /\A\d+\z/ then value.to_i
        when String then value
        else
          raise ParameterError, "unsupported load option value for #{opt}: #{value.inspect}"
        end
      end
    end

    def initialize(opts = [])
      @opts = opts
    end

    def key?(name)
      n = name.to_s
      @opts.any? {|opt| opt.name == n }
    end

    def each(&block)
      @opts.each(&block)
    end

    def []=(name, value)
      n = name.to_s
      delete n
      @opts.push Option.new(n, value)
    end

    def delete(name)
      @opts.reject! {|opt| opt.name == name }
    end

    def merge(pairs)
      h = {}
      @opts.each do |opt|
        h[opt.name] = opt
      end
      pairs.each do |key, value|
        h[key] = Option.new(key, value)
      end
      self.class.new(h.values)
    end

    def to_s
      buf = StringIO.new
      each do |opt|
        buf.puts opt
      end
      buf.string
    end

    class Option
      def initialize(name, value)
        @name = name
        @value = value
      end

      attr_reader :name
      attr_reader :value

      # Make polymorphic?
      def to_s
        case @value
        when true   # acceptanydate
          @name
        when false   # compupdate false
          "#{@name} false"
        when 'on', 'off'
          "#{@name} #{@value}"
        when String   # json 'auto'
          "#{@name} '#{@value}'"
        when Integer   # maxerror 10
          "#{@name} #{@value}"
        else
          raise ParameterError, "unsupported type of option value for #{@name}: #{@value.inspect}"
        end
      end
    end
  end

end
