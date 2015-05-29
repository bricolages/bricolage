JobClass.define('td-export') {
  parameters {|params|
    params.add SQLFileParam.new
    params.add DestFileParam.new
    params.add SrcTableParam.new
    params.add StringParam.new('src-database', 'NAME', 'Source TD database name.', optional: true)
    params.add EnumParam.new('format', %w(msgpack tsv csv json), 'Target file format.', default: 'msgpack')
    params.add OptionalBoolParam.new('gzip', 'If true, compresses target file by gzip.')
    params.add OptionalBoolParam.new('override', 'If true, clears target file.  Otherwise causes error.')
    params.add DataSourceParam.new('td')
  }

  parameters_filter {|job|
    job.provide_sql_file_by_job_id
  }

  declarations {|params|
    params['sql-file'].declarations
  }

  script {|params, script|
    script.task(params['data-source']) {|task|
      task.export params['sql-file'],
        path: params['dest-file'],
        format: params['format'],
        gzip: params['gzip'],
        override: params['override']
    }
  }
}
