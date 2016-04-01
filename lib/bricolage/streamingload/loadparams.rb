require 'bricolage/rubyjobclass'
require 'bricolage/psqldatasource'

module Bricolage

  module StreamingLoad

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

  end

end
