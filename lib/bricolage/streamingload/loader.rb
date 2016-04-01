require 'bricolage/sqlutils'
require 'bricolage/exception'
require 'bricolage/version'
require 'securerandom'
require 'json'
require 'forwardable'
require 'optparse'

module Bricolage

  module StreamingLoad

    class LoaderService

      def LoaderService.main
require 'pp'
require 'pry'
        opts = LoaderServiceOptions.new(ARGV)
        config_path, task_id = opts.parse

        config = YAML.load(File.read(config_path))
        ctx = Context.for_application('.')
        loader = new(
          context: ctx,
          data_source: ctx.get_data_source('sql', config['redshift-ds']),
          logger: ctx.logger
        )
        loader.execute_task task_id
      end

      def initialize(context:, data_source:, logger:)
        @ctx = context
        @ds = data_source
        @logger = logger
      end

      def execute_task(task_id)
        task = @ds.open {|conn| LoadTask.load(conn, task_id) }
        params = LoaderParams.load(@ctx, task)
        loader = Loader.new(@ctx, logger: @ctx.logger)
        loader.process(task, params)
      end

    end


    class LoaderServiceOptions

      def initialize(argv)
        @argv = argv
        @rest_arguments = nil
        @opts = opts = OptionParser.new
        opts.on('--task-id=ID', 'Execute oneshot load task (implicitly disables daemon mode).') {|task_id|
          @task_id = task_id
        }
        opts.on('--help', 'Prints this message and quit.') {
          puts opts.help
          exit 0
        }
        opts.on('--version', 'Prints version and quit.') {
          puts "#{File.basename($0)} version #{VERSION}"
          exit 0
        }
      end

      attr_reader :task_id
      attr_reader :rest_arguments

      def parse
        @opts.parse!(@argv)
        @rest_arguments = @argv.dup
      rescue OptionParser::ParseError => err
        raise OptionError, err.message
      end

    end


    class LoadTask

      def LoadTask.load(conn, task_id)
        rec = conn.query_row(<<-EndSQL)
          select
              t.dwh_task_seq
              , t.dwh_task_class
              , t.schema_name
              , t.table_name
              , prm.initialized
              , prm.disabled
          from
              dwh_tasks t
              left outer join dwh_str_load_tables prm using (schema_name, table_name)
          where
              t.dwh_task_id = '#{task_id}'
          ;
        EndSQL
        new(
          task_id: task_id,
          task_seq: rec['dwh_task_seq'],
          task_class: rec['dwh_task_class'],
          schema: rec['schema_name'],
          table: rec['table_name'],
          initialized: rec['initialized'],
          disabled: rec['disabled']
        )
      end

      def initialize(task_id:, task_seq:, task_class:,
          schema:, table:, initialized:, disabled:)
        @id = task_id
        @seq = task_seq
        @class = task_class
        @schema = schema
        @table = table
        @initialized = initialized
        @disabled = disabled
      end

      attr_reader :id
      attr_reader :seq
      attr_reader :schema
      attr_reader :table

      def initialized?
        @initialized
      end

      def disabled?
        @disabled
      end

    end


    class LoaderParams

      def LoaderParams.load(ctx, task)
        job = load_job(ctx, task)
        job.provide_default 'dest-table', "#{task.schema}.#{task.table}"
        #job.provide_sql_file_by_job_id   # FIXME: provide only when exist
        job.compile
        new(job)
      end

      def LoaderParams.load_job(ctx, task)
        if job_file = find_job_file(ctx, task)
          ctx.logger.debug "using .job file: #{job_file}"
          Job.load_file(job_file, ctx.subsystem(task.schema))
        else
          ctx.logger.debug "using default job parameters (no .job file)"
          Job.instantiate(task.table, 'streaming_load_v3', ctx).tap {|job|
            job.bind_parameters({})
          }
        end
      end

      def LoaderParams.find_job_file(ctx, task)
        paths = Dir.glob("#{ctx.home_path}/#{task.schema}/#{task.table}.*")
        paths.select {|path| File.extname(path) == '.job' }.sort.first
      end

      def initialize(job)
        @job = job
        @params = job.params
      end

      def ds
        @params['redshift-ds']
      end

      def ctl_bucket
        @params['ctl-ds']
      end

      def enable_work_table?
        !!@params['work-table']
      end

      def work_table
        @params['work-table']
      end

      def dest_table
        @params['dest-table']
      end

      def load_options_string
        @params['load-options'].to_s
      end

      def sql_source
        sql = @params['sql-file']
        sql ? sql.source : "insert into #{dest_table} select * from #{work_table};"
      end

    end


    require 'bricolage/rubyjobclass'
    require 'bricolage/psqldatasource'

    class LoaderJob < RubyJobClass

      job_class_id 'streaming_load_v3'

      def self.parameters(params)
        params.add DestTableParam.new(optional: false)
        params.add DestTableParam.new('work-table', optional: true)
        params.add KeyValuePairsParam.new('load-options', 'OPTIONS', 'Loader options.',
            optional: true, default: DEFAULT_LOAD_OPTIONS,
            value_handler: lambda {|value, ctx, vars| PSQLLoadOptions.parse(value) })
        params.add SQLFileParam.new('sql-file', 'PATH', 'SQL to insert rows from the work table to the target table.', optional: true)
        params.add DataSourceParam.new('sql', 'redshift-ds', 'Target data source.')
        params.add DataSourceParam.new('s3', 'ctl-ds', 'Manifest file data source.')
      end

      def self.default_load_options
      end

      # Use loosen options by default
      default_options = [
        ['json', 'auto'],
        ['gzip', true],
        ['timeformat', 'auto'],
        ['dateformat', 'auto'],
        ['acceptanydate', true],
        ['acceptinvchars', ' '],
        ['truncatecolumns', true],
        ['trimblanks', true]
      ]
      opts = default_options.map {|name, value| PSQLLoadOptions::Option.new(name, value) }
      DEFAULT_LOAD_OPTIONS = PSQLLoadOptions.new(opts)

      def self.declarations(params)
        Bricolage::Declarations.new(
          'dest_table' => nil,
          'work_table' => nil
        )
      end

      def initialize(params)
        @params = params
      end

      def bind(ctx, vars)
        @params['sql-file'].bind(ctx, vars) if @params['sql-file']
      end

    end


    class Loader

      include SQLUtils

      def initialize(ctx, logger:)
        @ctx = ctx
        @logger = logger

        @job_id = SecureRandom.uuid
        @job_seq = nil
        @start_time = Time.now
        @end_time = nil
      end

      def process(task, params)
        params.ds.open {|conn|
          @connection = conn
          assign_task task
          do_load task, params
        }
      end

      def assign_task(task)
        @connection.transaction {
          @connection.lock('dwh_tasks')
          @connection.lock('dwh_jobs')
          @connection.lock('dwh_job_results')

          processed = task_processed?(task)
          table_in_use = table_in_use?(task)
          write_job(task.seq)
          @job_seq = read_job_seq(@job_id)
          @logger.info "dwh_job_seq=#{@job_seq}"
          abort_job 'failure', "task is already processed by other jobs" if processed
          abort_job 'failure', "other jobs are working on the target table" if table_in_use
        }
      end

      def do_load(task, params)
        staged_files = stage_files(task)
        return if staged_files.empty?
        ManifestFile.create(
          params.ctl_bucket,
          job_id: @job_id,
          object_urls: staged_files,
          logger: @logger
        ) {|manifest|
          if params.enable_work_table?
            prepare_work_table params.work_table
            load_objects params.work_table, manifest, params.load_options_string
            @connection.transaction {
              commit_work_table params
              commit_job_result
            }
          else
            @connection.transaction {
              load_objects params.dest_table, manifest, params.load_options_string
              commit_job_result
            }
          end
        }
      rescue JobFailure => ex
        write_job_error 'failure', ex.message
        raise
      rescue Exception => ex
        write_job_error 'error', ex.message
        raise
      end

      def write_job(task_seq)
        @connection.execute(<<-EndSQL)
            insert into dwh_jobs
                ( dwh_job_id
                , job_class
                , utc_start_time
                , process_id
                , dwh_task_seq
                )
            values
                ( #{s @job_id}
                , 'streaming_load_v3'
                , #{t @start_time.getutc}
                , #{$$}
                , #{task_seq}
                )
            ;
        EndSQL
      end

      def read_job_seq(job_id)
        @connection.query_value(<<-EndSQL)
            select dwh_job_seq
            from dwh_jobs
            where dwh_job_id = #{s job_id}
            ;
        EndSQL
      end

      def task_processed?(task)
        n_nonerror_jobs = @connection.query_value(<<-EndSQL)
            select
                count(*)
            from
                dwh_tasks t
                inner join dwh_jobs j using (dwh_task_seq)
                left outer join dwh_job_results r using (dwh_job_seq)
            where
                t.dwh_task_seq = #{task.seq}
                and (
                    r.status is null         -- running
                    or r.status = 'success'  -- finished with success
                )
            ;
        EndSQL
        @logger.debug "n_nonerror_jobs=#{n_nonerror_jobs.inspect}"
        n_nonerror_jobs.to_i > 0
      end

      def table_in_use?(task)
        n_working_jobs = @connection.query_value(<<-EndSQL)
            select
                count(*)
            from
                dwh_tasks t
                inner join dwh_jobs j using (dwh_task_seq)
                left outer join dwh_job_results r using (dwh_job_seq)
            where
                t.schema_name = #{s task.schema}
                and t.table_name = #{s task.table}
                and r.status is null   -- running
            ;
        EndSQL
        @logger.debug "n_working_jobs=#{n_working_jobs.inspect}"
        n_working_jobs.to_i > 0
      end

      def stage_files(task)
        # already-processed object_url MUST be removed
        @connection.execute(<<-EndSQL)
            insert into dwh_str_load_files
                ( dwh_job_seq
                , object_url
                )
            select
                #{@job_seq}
                , object_url
            from
                dwh_str_load_files_incoming
            where
                dwh_task_seq = #{task.seq}
                and object_url not in (
                    select
                        f.object_url
                    from
                        dwh_str_load_files f
                        inner join dwh_job_results r using (dwh_job_seq)
                    where
                        r.status = 'success'
                )
            ;
        EndSQL

        staged_files = @connection.query_values(<<-EndSQL)
            select
                object_url
            from
                dwh_str_load_files
            where
                dwh_job_seq = #{@job_seq}
            ;
        EndSQL
        @logger.debug "n_staged_files=#{staged_files.size}"

        staged_files
      end

      def prepare_work_table(work_table)
        @connection.execute("truncate #{work_table}")
      end

      def load_objects(dest_table, manifest, options)
        @connection.execute(<<-EndSQL.strip.gsub(/\s+/, ' '))
            copy #{dest_table}
            from '#{manifest.url}'
            credentials '#{manifest.credential_string}'
            manifest
            statupdate false
            compupdate false
            #{options}
            ;
        EndSQL
        @logger.info "load succeeded: #{manifest.url}"
      end

      def commit_work_table(params)
        @connection.execute(params.sql_source)
        # keep work table records for later tracking
      end

      def commit_job_result
        @end_time = Time.now
        write_job_result 'success', ''
      end

      MAX_MESSAGE_LENGTH = 1000

      def abort_job(status, message)
        write_job_error(status, message)
        raise JobFailure, message
      end

      def write_job_error(status, message)
        @end_time = Time.now
        write_job_result status, message.lines.first.strip[0, MAX_MESSAGE_LENGTH]
      end

      def write_job_result(status, message)
        @connection.execute(<<-EndSQL)
          insert into dwh_job_results
              ( dwh_job_seq
              , utc_end_time
              , status
              , message
              )
          values
              ( #{@job_seq}
              , #{t @end_time.getutc}
              , #{s status}
              , #{s message}
              )
          ;
        EndSQL
      end

    end


    class ManifestFile

      def ManifestFile.create(ds, job_id:, object_urls:, logger:, noop: false, &block)
        manifest = new(ds, job_id, object_urls, logger: logger, noop: noop)
        manifest.create_temporary(&block)
      end

      def initialize(ds, job_id, object_urls, logger:, noop: false)
        @ds = ds
        @job_id = job_id
        @object_urls = object_urls
        @logger = logger
        @noop = noop
      end

      def credential_string
        @ds.credential_string
      end

      def name
        @name ||= "manifest-#{@job_id}.json"
      end

      def url
        @url ||= @ds.url(name)
      end

      def content
        @content ||= begin
          ents = @object_urls.map {|url|
            { "url" => url, "mandatory" => true }
          }
          obj = { "entries" => ents }
          JSON.pretty_generate(obj)
        end
      end

      def put
        @logger.info "s3: put: #{url}"
        @ds.object(name).put(body: content) unless @noop
      end

      def delete
        @logger.info "s3: delete: #{url}"
        @ds.object(name).delete unless @noop
      end

      def create_temporary
        put
        yield self
        delete
      end

    end

  end

end
