require 'bricolage/rubyjobclass'
require 'bricolage/psqldatasource'
require 'bricolage/exception'
require 'json'
require 'socket'

class StreamingLoadJobClass < RubyJobClass
  job_class_id 'streaming_load'

  def StreamingLoadJobClass.parameters(params)
    super
    params.add Bricolage::DataSourceParam.new('sql', 'redshift-ds', 'Redshift data source.')
    params.add Bricolage::DestTableParam.new(optional: false)
    params.add Bricolage::DestTableParam.new('work-table', optional: true)
    params.add Bricolage::DestTableParam.new('log-table', optional: true)
    params.add Bricolage::KeyValuePairsParam.new('load-options', 'OPTIONS', 'Loader options.',
        optional: true, default: Bricolage::PSQLLoadOptions.new,
        value_handler: lambda {|value, ctx, vars| Bricolage::PSQLLoadOptions.parse(value) })
    params.add Bricolage::DataSourceParam.new('s3', 's3-ds', 'S3 data source.')
    params.add Bricolage::StringParam.new('queue-path', 'S3_PATH', 'S3 path for data file queue.')
    params.add Bricolage::StringParam.new('persistent-path', 'S3_PATH', 'S3 path for persistent data file store.')
    params.add Bricolage::StringParam.new('file-name', 'PATTERN', 'name pattern of target data file.')
    params.add Bricolage::SQLFileParam.new('sql-file', 'PATH', 'SQL to insert rows from the work table to the target table.', optional: true)
    params.add Bricolage::OptionalBoolParam.new('noop', 'Does not change any data.')
    params.add Bricolage::OptionalBoolParam.new('load-only', 'Just issues COPY statement to work table and quit. No INSERT, no dequeue, no load log.')
    params.add Bricolage::OptionalBoolParam.new('dequeue-only', 'Dequeues already loaded files.')
  end

  def StreamingLoadJobClass.declarations(params)
    Bricolage::Declarations.new(
      'dest_table' => nil,
      'work_table' => nil,
      'log_table' => nil
    )
  end

  def initialize(params)
    @loader = make_loader(params)
    @load_only = params['load-only']
    @dequeue_only = params['dequeue-only']
  end

  def run
    @loader.dequeue_loaded_files unless @load_only
    return nil if @dequeue_only
    @loader.load
    nil
  end

  def bind(ctx, vars)
    @loader.sql.bind(ctx, vars) if @loader.sql
  end

  def make_loader(params)
    ds = params['redshift-ds']
    RedshiftStreamingLoader.new(
      data_source: ds,
      queue: make_s3_queue(params),
      table: string(params['dest-table']),
      work_table: string(params['work-table']),
      log_table: string(params['log-table']),
      load_options: params['load-options'],
      sql: params['sql-file'],
      logger: ds.logger,
      noop: params['noop'],
      load_only: params['load-only']
    )
  end

  def make_s3_queue(params)
    ds = params['s3-ds']
    S3Queue.new(
      data_source: ds,
      queue_path: params['queue-path'],
      persistent_path: params['persistent-path'],
      file_name: params['file-name'],
      logger: ds.logger
    )
  end

  def string(obj)
    obj ? obj.to_s : nil
  end

  class RedshiftStreamingLoader
    def initialize(data_source:, queue:,
        table:, work_table: nil, log_table: nil, load_options: nil,
        sql: nil,
        logger:, noop: false, load_only: false)
      @ds = data_source
      @src = queue
      @table = table
      @work_table = work_table
      @log_table = log_table
      @load_options = load_options
      @sql = sql
      @logger = logger
      @noop = noop
      @load_only = load_only

      @start_time = Time.now
      @end_time = nil
      @job_process_id = "#{@start_time.strftime('%Y%m%d-%H%M%S')}.#{Socket.gethostname}.#{Process.pid}"
    end

    attr_reader :sql

    def load
      load_in_parallel
    end

    def log_basic_info
      @logger.info "start_time: #{@start_time}"
      @logger.info "job_process_id: #{@job_process_id}"
      @logger.info "queue: #{@src.queue_url}"
    end

    def load_in_parallel
      log_basic_info
      @logger.info 'load with manifest'
      objects = @src.queued_objects
      if objects.empty?
        @logger.info 'no target data files; exit'
        return
      end
      create_manifest_file(objects) {|manifest_url|
        @ds.open {|conn|
          init_work_table conn
          execute_update conn, copy_manifest_statement(manifest_url, @src.credential_string)
          @logger.info "load succeeded: #{manifest_url}" unless @noop
          commit conn, objects
        }
        dequeue_all objects
      }
    end

    def load_in_sequential
      log_basic_info
      @logger.info 'load each objects sequentially'
      objects = @src.queued_objects
      @ds.open {|conn|
        init_work_table(conn)
        objects.each do |obj|
          @logger.info "load: #{obj.url}"
          execute_update conn, copy_file_statement(obj)
          @logger.info "load succeeded: #{obj.url}" unless @noop
        end
        commit conn, objects
      }
      dequeue_all objects
    end

    def commit(conn, objects)
      @end_time = Time.now
      return if @load_only
      transaction(conn) {
        commit_work_table conn
        write_load_logs conn, objects
      }
    end

    def dequeue_loaded_files
      @logger.info "dequeue start"
      objects = @src.queued_objects
      @ds.open {|conn|
        objects.each do |obj|
          if loaded_object?(conn, obj)
            obj.dequeue(@noop)
          end
        end
      }
    end

    private

    def init_work_table(conn)
      return unless @work_table
      execute_update conn, "truncate #{@work_table};"
    end

    def commit_work_table(conn)
      return unless @work_table
      insert_stmt = @sql ? @sql.source : "insert into #{@table} select * from #{@work_table};"
      execute_update conn, insert_stmt
      # keep work table records for tracing
    end

    def copy_file_statement(obj)
      %Q(
        copy #{load_target_table} from '#{obj.url}'
        credentials '#{obj.credential_string}'
        #{@load_options}
      ;).gsub(/\s+/, ' ').strip
    end

    def create_manifest_file(objects)
      manifest_name = "manifest-#{@job_process_id}.json"
      @logger.info "creating manifest: #{manifest_name}"
      @logger.info "manifest:\n" + make_manifest_json(objects)
      url = @src.put_control_file(manifest_name, make_manifest_json(objects), noop: @noop)
      yield url
      @src.remove_control_file(File.basename(url), noop: @noop)
    end

    def make_manifest_json(objects)
      ents = objects.map {|obj|
        { "url" => obj.url, "mandatory" => true }
      }
      JSON.pretty_generate({ "entries" => ents })
    end

    def copy_manifest_statement(manifest_url, credential_string)
      %Q(
        copy #{load_target_table}
        from '#{manifest_url}'
        credentials '#{credential_string}'
        manifest
        #{@load_options}
      ;).gsub(/\s+/, ' ').strip
    end

    def load_target_table
      @work_table || @table
    end

    def write_load_logs(conn, objects)
      return unless @log_table
      make_load_logs(objects).each do |record|
        write_load_log conn, record
      end
    end

    def make_load_logs(objects)
      objects.map {|obj| make_load_log(obj) }
    end

    def make_load_log(obj)
      LoadLogRecord.new(@job_process_id, @start_time, @end_time, @table, obj.url)
    end

    LoadLogRecord = Struct.new(:job_process_id, :start_time, :end_time, :target_table, :data_file)

    def write_load_log(conn, record)
      return unless @log_table
      execute_update(conn, <<-EndSQL.gsub(/^\s+/, '').strip)
        insert into #{@log_table}
        ( job_process_id
        , start_time
        , end_time
        , target_table
        , data_file
        )
        values
        ( #{sql_string record.job_process_id}
        , #{sql_timestamp record.start_time}
        , #{sql_timestamp record.end_time}
        , #{sql_string record.target_table}
        , #{sql_string record.data_file}
        )
        ;
      EndSQL
    end

    def loaded_object?(conn, obj)
      rs = conn.execute("select count(*) as c from #{@log_table} where data_file = #{sql_string obj.url}")
      rs.first['c'].to_i > 0
    end

    def sql_timestamp(time)
      %Q(timestamp '#{time.strftime('%Y-%m-%d %H:%M:%S')}')
    end

    def sql_string(str)
      escaped = str.gsub("'", "''")
      %Q('#{escaped}')
    end

    def transaction(conn)
      execute_update conn, 'begin transaction'
      yield
      execute_update conn, 'commit'
    end

    def execute_update(conn, sql)
      if @noop
        log_query(sql)
      else
        conn.execute(sql)
      end
    end

    def log_query(sql)
      @logger.info "[#{@ds.name}] #{mask_secrets(sql)}"
    end

    def mask_secrets(log)
      log.gsub(/\bcredentials\s+'.*?'/mi, "credentials '****'")
    end

    def dequeue_all(objects)
      return if @load_only
      objects.each do |obj|
        obj.dequeue(@noop)
      end
    end
  end

  class S3Queue
    def initialize(data_source:, queue_path:, persistent_path:, file_name:, logger:)
      @ds = data_source
      @queue_path = queue_path
      @persistent_path = persistent_path
      @file_name = file_name
      @logger = logger
    end

    def credential_string
      @ds.credential_string
    end

    attr_reader :queue_path

    def queue_url
      @ds.url(@queue_path)
    end

    def object_url(key)
      @ds.url(key, no_prefix: true)
    end

    def control_file_url(name)
      @ds.url(control_file_path(name))
    end

    def put_control_file(name, data, noop: false)
      @logger.info "s3 put: #{control_file_url(name)}"
      @ds.object(control_file_path(name)).write(data) unless noop
      control_file_url(name)
    end

    def remove_control_file(name, noop: false)
      @logger.info "s3 delete: #{control_file_url(name)}"
      @ds.object(control_file_path(name)).delete unless noop
    end

    def control_file_path(name)
      "#{queue_path}/#{name}"
    end

    def consume_each(noop: false, &block)
      each do |obj|
        yield obj and obj.save(noop: noop)
      end
    end

    def each(&block)
      queued_objects.each(&block)
    end

    def queue_directory
      @ds.objects_with_prefix(queue_path)
    end

    def queued_file_nodes
      queue_directory.as_tree.children.select {|node|
        node.leaf? and
          node.key[-1, 1] != '/' and
          target_file_name?(File.basename(node.key))
      }
    end

    def queued_objects
      queued_file_nodes.map {|node| LoadableObject.new(self, node, @logger) }
    end

    def target_file_name?(name)
      file_name_pattern =~ name
    end

    def persistent_path(name)
      @ds.path("#{format_path(@persistent_path, name)}/#{name}")
    end

    def format_path(template, basename)
      m = file_name_pattern.match(basename) or
        raise ParameterError, "file name does not match the pattern: #{basename.inspect}"
      template.gsub(/%./) {|op|
        case op
        when '%Y' then m[:year]
        when '%m' then m[:month]
        when '%d' then m[:day]
        when '%H' then m[:hour]
        when '%M' then m[:minute]
        when '%S' then m[:second]
        when '%N' then m[:nanosecond]
        when '%Q' then m[:seq]
        else raise ParameterError, "unknown time format in s3.file_name config: #{op}"
        end
      }
    end

    def file_name_pattern
      @file_name_pattern ||= compile_name_pattern(@file_name)
    end

    def compile_name_pattern(template)
      pattern = template.gsub(/[^%]+|%\d*./) {|op|
        case op
        when '%Y' then '(?<year>\\d{4})'
        when '%m' then '(?<month>\\d{2})'
        when '%d' then '(?<day>\\d{2})'
        when '%H' then '(?<hour>\\d{2})'
        when '%M' then '(?<minute>\\d{2})'
        when '%S' then '(?<second>\\d{2})'
        when /\A%(\d+)N\z/ then "(?<nanosecond>\\d{#{$1}})"
        when '%Q' then '(?<seq>\\d+)'
        when '%*' then '[^/]*'
        when '%%' then '%'
        when /\A%/ then raise ParameterError, "unknown time format in s3.file_name config: #{op.inspect}"
        else Regexp.quote(op)
        end
      }
      Regexp.compile("\\A#{pattern}\\z")
    end
  end

  class LoadableObject
    def initialize(s3queue, node, logger)
      @s3queue = s3queue
      @node = node
      @logger = logger
    end

    def credential_string
      @s3queue.credential_string
    end

    def path
      @node.key
    end

    def basename
      File.basename(path)
    end

    def url
      @s3queue.object_url(path)
    end

    def save(noop = false)
      @logger.info "s3 move: #{path} -> #{save_path}"
      return if noop
      @node.object.move_to save_path
      @logger.info "file saved"
    end

    alias dequeue save

    def save_path
      @s3queue.persistent_path(basename)
    end
  end
end
