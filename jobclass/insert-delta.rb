JobClass.define('insert-delta') {
  parameters {|params|
    params.add SQLFileParam.new
    params.add DestTableParam.new
    params.add SrcTableParam.new
    params.add StringParam.new('delete-cond', 'SQL_EXPR', 'DELETE condition.')
    params.add OptionalBoolParam.new('analyze', 'ANALYZE table after SQL is executed.')
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
      task.transaction {
        task.exec SQLStatement.delete_where(params['delete-cond'])
        task.exec params['sql-file']
      }
      task.vacuum_if params['vacuum'], params['vacuum-sort']
      task.analyze_if params['analyze']
    }
  }
}
