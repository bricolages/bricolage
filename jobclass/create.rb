JobClass.define('create') {
  parameters {|params|
    params.add SQLFileParam.new('table-def', 'PATH', 'CREATE TABLE file.')
    params.add DestTableParam.new
    params.add OptionalBoolParam.new('drop', 'DROP table before CREATE.')
    params.add OptionalBoolParam.new('analyze', 'ANALYZE table after SQL is executed.')
    params.add KeyValuePairsParam.new('grant', 'KEY:VALUE', 'GRANT table after SQL is executed. (required keys: privilege, to)')
    params.add DataSourceParam.new('sql')
  }

  declarations {|params|
    params['table-def'].declarations
  }

  script {|params, script|
    script.task(params['data-source']) {|task|
      task.transaction {
        task.drop_force_if params['drop']
        task.exec params['table-def']
        task.analyze_if params['analyze']
        task.grant_if params['grant'], params['dest-table']
      }
    }
  }
}
