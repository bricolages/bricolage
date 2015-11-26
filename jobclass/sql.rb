JobClass.define('sql') {
  parameters {|params|
    params.add SQLFileParam.new
    params.add DestTableParam.new
    params.add SrcTableParam.new
    params.add OptionalBoolParam.new('truncate', 'TRUNCATE table before SQL is executed.')
    params.add OptionalBoolParam.new('analyze', 'ANALYZE table after SQL is executed.')
    params.add OptionalBoolParam.new('vacuum', 'VACUUM table after SQL is executed.')
    params.add OptionalBoolParam.new('vacuum-sort', 'VACUUM SORT table after SQL is executed.')
    params.add KeyValuePairsParam.new('grant', 'KEY:VALUE', 'GRANT table after SQL is executed. (required keys: privilege, to)')
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
      task.exec params['sql-file']
      task.vacuum_if params['vacuum'], params['vacuum-sort']
      task.analyze_if params['analyze']
      task.grant_if params['grant'], params['dest-table']
    }
  }
}
