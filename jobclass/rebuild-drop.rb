JobClass.define('rebuild-drop') {
  parameters {|params|
    params.add SQLFileParam.new
    params.add DestTableParam.new
    params.add SrcTableParam.new
    params.add SQLFileParam.new('table-def', 'PATH', 'Create table file.')
    params.add OptionalBoolParam.new('analyze', 'ANALYZE table after SQL is executed.', default: true)
    params.add OptionalBoolParam.new('vacuum', 'VACUUM table after SQL is executed.')
    params.add OptionalBoolParam.new('vacuum-sort', 'VACUUM SORT table after SQL is executed.')
    params.add KeyValuePairsParam.new('grant', 'KEY:VALUE', 'GRANT table after SQL is executed. (required keys: on, to)')
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
      # CREATE
      task.drop_force params['dest-table']
      task.exec params['table-def']

      # INSERT
      task.exec params['sql-file']

      # VACUUM, ANALYZE, GRANT
      task.vacuum_if params['vacuum'], params['vacuum-sort'], params['dest-table']
      task.analyze_if params['analyze'], params['dest-table']
      task.grant_if params['grant'], params['dest-table']
    }
  }
}
