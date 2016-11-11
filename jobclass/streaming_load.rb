require 'bricolage/rubyjobclass'
require 'bricolage/psqldatasource'
require 'bricolage/exception'
require 'json'
require 'socket'
require 'forwardable'

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
    params.add Bricolage::StringParam.new('ctl-prefix', 'S3_PREFIX', 'S3 object key prefix for control files. (default: ${queue-path}/ctl)', optional: true)
    params.add Bricolage::OptionalBoolParam.new('keep-ctl', 'Does not delete control files if true.')
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
    if @dequeue_only
      @loader.dequeue
    else
      @loader.load
    end
    nil
  end

  def bind(ctx, vars)
    @loader.sql.bind(ctx, vars) if @loader.sql
  end

  def make_loader(params)
    ds = params['redshift-ds']
    load_opts = params['load-options']
    load_opts.provide_defaults(params['s3-ds'])
    RedshiftStreamingLoader.new(
      data_source: ds,
      queue: make_s3_queue(params),
      keep_ctl: params['keep-ctl'],
      table: string(params['dest-table']),
      work_table: string(params['work-table']),
      log_table: string(params['log-table']),
      load_options: load_opts,
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
      ctl_prefix: (params['ctl-prefix'] || "#{params['queue-path']}/ctl"),
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
    def initialize(data_source:, queue:, keep_ctl:,
        table:, work_table: nil, log_table: nil, load_options: nil,
        sql: nil,
        logger:, noop: false, load_only: false)
      @ds = data_source
      @src = queue
      @keep_ctl = keep_ctl
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

    def work_table
      @work_table || "#{@table}_wk"
    end

    def log_table
      @log_table || "#{@table}_l"
    end

    def log_basic_info
      @logger.info "start_time: #{@start_time}"
      @logger.info "job_process_id: #{@job_process_id}"
      @logger.info "queue: #{@src.queue_url}"
    end

    def dequeue
      log_basic_info
      @logger.info "dequeue start"
      objects = @src.queued_objects
      if objects.empty?
        @logger.info 'no target data files; exit'
        return
      end
      create_load_log_file(objects) {|log_url|
        @ds.open {|conn|
          execute_update conn, "delete #{log_table_wk};"
          execute_update conn, load_log_copy_stmt(log_table_wk, log_url, @src.credential_string)
          loaded, not_loaded = partition_loaded_objects(conn, objects, log_table_wk)
          loaded.each do |obj|
            obj.dequeue(force: true, noop: @noop)
          end
        }
      }
    end

    def load
      log_basic_info
      @logger.info 'streaming load start'
      objects = @src.queued_objects
      if objects.empty?
        @logger.info 'no target data files; exit'
        return
      end
      create_load_log_file(objects) {|log_url|
        @ds.open {|conn|
          execute_update conn, "truncate #{work_table}"
          conn.transaction {|txn|
            execute_update conn, "delete #{log_table_wk}"
            execute_update conn, load_log_copy_stmt(log_table_wk, log_url, @src.credential_string)
            loaded, not_loaded = partition_loaded_objects(conn, objects, log_table_wk)
            unless @load_only
              loaded.each do |obj|
                obj.dequeue(force: true, noop: @noop)
              end
            end
            if not_loaded.empty?
              @logger.info "no data files to load" unless @noop
              txn.truncate_and_commit log_table_wk
            else
              create_manifest_file(not_loaded) {|manifest_url|
                execute_update conn, manifest_copy_stmt(work_table, manifest_url)
                @logger.info "load succeeded: #{manifest_url}" unless @noop
                unless @load_only
                  commit_work_table conn, work_table
                  commit_load_log conn, log_table_wk
                end
                txn.truncate_and_commit log_table_wk
              }
              unless @load_only
                not_loaded.each do |obj|
                  obj.dequeue(force: true, noop: @noop)
                end
              end
            end
          }
        }
      }
    end

    private

    def commit_work_table(conn, work_table)
      insert_stmt = @sql ? @sql.source : "insert into #{@table} select * from #{work_table};"
      execute_update conn, insert_stmt
    end

    def create_manifest_file(objects)
      manifest_name = if @keep_ctl
          "#{Time.now.strftime('%Y/%m/%d')}/#{@job_process_id}-#{@table}.json"
        else
          "manifest-#{@job_process_id}.json"
        end
      @logger.info "creating manifest: #{manifest_name}"
      json = make_manifest_json(objects)
      @logger.info "manifest:\n" + json
      url = @src.put_control_file(manifest_name, json, noop: @noop)
      yield url
      @src.remove_control_file(File.basename(url), noop: @noop) unless @keep_ctl
    end

    def make_manifest_json(objects)
      ents = objects.map {|obj|
        { "url" => obj.url, "mandatory" => false }
      }
      JSON.pretty_generate({ "entries" => ents })
    end

    def manifest_copy_stmt(target_table, manifest_url)
      %Q(
        copy #{target_table}
        from '#{manifest_url}'
        credentials '#{@src.credential_string}'
        manifest
        compupdate false
        statupdate false
        #{@load_options}
      ;).gsub(/\s+/, ' ').strip
    end

    def create_load_log_file(objects)
      log_name = "load_log-#{@job_process_id}.csv"
      @logger.info "creating tmp load log: #{log_name}"
      csv = make_load_log_csv(objects)
      @logger.info "load_log:\n" + csv
      url = @src.put_control_file(log_name, csv, noop: @noop)
      begin
        yield url
      ensure
        @src.remove_control_file(File.basename(url), noop: @noop)
      end
    end

    def make_load_log_csv(objects)
      buf = StringIO.new
      objects.each do |obj|
        log = make_load_log(obj)
        cols = [
          log.job_process_id,
          format_timestamp(log.start_time),
          '',    # end time does not exist yet
          log.target_table,
          log.data_file
        ]
        buf.puts cols.map {|c| %Q("#{c}") }.join(',')
      end
      buf.string
    end

    def make_load_log(obj)
      LoadLogRecord.new(@job_process_id, @start_time, @end_time, @table, obj.url)
    end

    LoadLogRecord = Struct.new(:job_process_id, :start_time, :end_time, :target_table, :data_file)

    def create_tmp_log_table(conn, log_url)
    end

    def log_table_wk
      "#{@log_table}_wk"
    end

    def load_log_copy_stmt(target_table, log_url, credential_string)
      %Q(
        copy #{target_table}
        from '#{log_url}'
        credentials '#{credential_string}'
        delimiter ','
        removequotes
        compupdate false
        statupdate false
      ;).gsub(/\s+/, ' ').strip
    end

    def partition_loaded_objects(conn, objects, tmp_log_table)
      recs = conn.execute(<<-EndSQL)
        select
            data_file
            , case when l.job_process_id is not null then 'true' else 'false' end as is_loaded
        from
            #{@log_table} l right outer join #{tmp_log_table} t using (data_file)
        ;
      EndSQL
      index = {}
      objects.each do |obj|
        index[obj.url] = obj
      end
      recs.each do |rec|
        obj = index[rec['data_file']]
        obj.loaded = (rec['is_loaded'] == 'true')
      end
      objects.partition(&:loaded)
    end

    def commit_load_log(conn, tmp_table_name)
      @end_time = Time.now
      conn.execute(<<-EndSQL)
        insert into #{@log_table}
        select
            job_process_id
            , start_time
            , #{sql_timestamp @end_time}
            , target_table
            , data_file
        from
            #{tmp_table_name}
        where
            data_file not in (select data_file from #{@log_table})
        ;
      EndSQL
    end

    def sql_timestamp(time)
      %Q(timestamp '#{format_timestamp(time)}')
    end

    def format_timestamp(time)
      time.strftime('%Y-%m-%d %H:%M:%S')
    end

    def sql_string(str)
      escaped = str.gsub("'", "''")
      %Q('#{escaped}')
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
  end

  class S3Queue
    extend Forwardable

    def initialize(data_source:, ctl_prefix:, queue_path:, persistent_path:, file_name:, logger:)
      @ds = data_source
      @ctl_prefix = ctl_prefix
      @queue_path = queue_path
      @persistent_path = persistent_path
      @file_name = file_name
      @logger = logger
    end

    def credential_string
      @ds.credential_string
    end

    def_delegator '@ds', :encryption

    attr_reader :queue_path

    def queue_url
      @ds.url(@queue_path)
    end

    def object_url_direct(key)
      @ds.url(key, no_prefix: true)
    end

    def control_file_url(name)
      @ds.url(control_file_path(name))
    end

    def put_control_file(name, data, noop: false)
      @logger.info "s3 put: #{control_file_url(name)}"
      @ds.object(control_file_path(name)).put(body: data) unless noop
      control_file_url(name)
    end

    def remove_control_file(name, noop: false)
      @logger.info "s3 delete: #{control_file_url(name)}"
      @ds.object(control_file_path(name)).delete unless noop
    end

    def control_file_path(name)
      prefix = @ctl_prefix || "#{queue_path}/ctl"
      "#{prefix}/#{name}"
    end

    def each(&block)
      queued_objects.each(&block)
    end

    def queued_objects
      @ds.traverse(queue_path)
          .select {|obj| target_file_name?(File.basename(obj.key)) }
          .map {|obj| LoadableObject.new(self, obj, @logger) }
    end

    def target_file_name?(name)
      file_name_pattern =~ name
    end

    def persistent_object(name)
      @ds.object(persistent_path(name), no_prefix: true)
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
    def initialize(s3queue, object, logger)
      @s3queue = s3queue
      @object = object
      @logger = logger
      @loaded = nil
    end

    attr_accessor :loaded

    def credential_string
      @s3queue.credential_string
    end

    def path
      @object.key
    end

    def basename
      File.basename(path)
    end

    def url
      @s3queue.object_url_direct(path)
    end

    def dequeue(force: false, noop: false)
      @logger.info "s3 move: #{path} -> #{persistent_path}"
      return if noop
      @object.move_to persistent_object, dequeue_options
      @logger.info "done"
    rescue Aws::S3::Errors::NoSuchKey => ex
      @logger.error "S3 error: #{ex.message}"
      if force
        @logger.info "move error ignored (may be caused by eventual consistency)"
      else
        raise
      end
    end

    def persistent_object
      @s3queue.persistent_object(basename)
    end

    def persistent_path
      @s3queue.persistent_path(basename)
    end

    def dequeue_options
      opts = {
        server_side_encryption: @s3queue.encryption
      }
      opts.reject {|k,v| v.nil? }
    end
  end
end
