JobClass.define('adhoc') {
  parameters {|params|
    params.add SQLFileParam.new
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
      task.exec params['sql-file']
    }
  }
}
