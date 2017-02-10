JobClass.define('insert') {
  parameters {|params|
    params.add SQLFileParam.new
    params.add DestTableParam.new
    params.add SrcTableParam.new
    params.add SQLFileParam.new('table-def', 'PATH', 'Create table file.', optional: true)
    params.add OptionalBoolParam.new('drop', 'DROP table before CREATE.')
    params.add OptionalBoolParam.new('truncate', 'TRUNCATE table before SQL is executed.')
    params.add OptionalBoolParam.new('analyze', 'ANALYZE table after SQL is executed.', default: true)
    params.add OptionalBoolParam.new('vacuum', 'VACUUM table after SQL is executed.')
    params.add OptionalBoolParam.new('vacuum-sort', 'VACUUM SORT table after SQL is executed.')
    params.add DataSourceParam.new('sql')
  }

  parameters_filter {|job|
    job.provide_sql_file_by_job_id
  }

  declarations {|params|
    params['sql-file'].declarations
  }

  script {|params, script|
    script.task(params['data-source']) {|task|
      task.truncate_if params['truncate']
      task.transaction {
        task.drop_force_if params['drop']
        task.exec params['table-def'] if params['table-def']
        task.exec params['sql-file']
        task.analyze_if params['analyze']
      }
      task.vacuum_if params['vacuum'], params['vacuum-sort']
    }
  }
}
