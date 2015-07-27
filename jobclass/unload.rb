require 'bricolage/psqldatasource'

JobClass.define('unload') {
  parameters {|params|
    params.add SQLFileParam.new
    params.add DataSourceParam.new('sql', 'src-ds', 'Input data source (sql).')
    params.add SrcTableParam.new(optional: false)
    params.add DataSourceParam.new('s3', 'dest-ds', 'Target data source (s3).')
    params.add DestFileParam.new
    params.add EnumParam.new('format', %w(tsv csv), 'Data file format.', default: 'tsv')
    params.add KeyValuePairsParam.new('options', 'OPTIONS', 'Loader options.',
        optional: true, default: PSQLLoadOptions.new,
        value_handler: lambda {|value, ctx, vars| PSQLLoadOptions.parse(value) })
  }

  parameters_filter {|job|
    job.provide_sql_file_by_job_id
  }

  declarations {|params|
    params['sql-file'].declarations
  }

  script {|params, script|
    script.task(params['src-ds']) {|task|
      task.unload params['sql-file'], params['dest-ds'], params['dest-file'],
          params['format'], params['options']
    }
  }
}
