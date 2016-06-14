JobClass.define('rebuild-rename') {
  parameters {|params|
    params.add SQLFileParam.new
    params.add DestTableParam.new
    params.add SrcTableParam.new
    params.add SQLFileParam.new('table-def', 'PATH', 'Create table file.')
    params.add OptionalBoolParam.new('analyze', 'ANALYZE table after SQL is executed.', default: true)
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
      prev_table = '${dest_table}_old'
      work_table = '${dest_table}_wk'

      task.transaction {
        # CREATE
        task.drop_force prev_table
        task.drop_force work_table
        task.exec params['table-def'].replace(/\$\{?dest_table\}?\b/, work_table)

        # INSERT
        task.exec params['sql-file'].replace(/\$\{?dest_table\}?\b/, work_table)

        # GRANT
        task.grant_if params['grant'], work_table
      }

      # VACUUM, ANALYZE
      task.vacuum_if params['vacuum'], params['vacuum-sort'], work_table
      task.analyze_if params['analyze'], work_table

      # RENAME
      task.transaction {
        task.create_dummy_table '$dest_table'
        dest_table = params['dest-table']
        task.rename_table dest_table.to_s, "#{dest_table.name}_old"
        task.rename_table "#{dest_table}_wk", dest_table.name
      }
    }
  }
}
