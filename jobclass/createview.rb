JobClass.define('createview') {
  parameters {|params|
    params.add SQLFileParam.new('sql-file', 'PATH', 'CREATE VIEW file.')
    params.add DestTableParam.new
    params.add SrcTableParam.new
    params.add OptionalBoolParam.new('drop', 'DROP table before CREATE.')
    params.add KeyValuePairsParam.new('grant', 'KEY:VALUE', 'GRANT table after SQL is executed. (required keys: privilege, to)')
    params.add DataSourceParam.new('sql')
  }

  declarations {|params|
    params['sql-file'].declarations
  }

  script {|params, script|
    script.task(params['data-source']) {|task|
      task.drop_view_force_if params['drop']
      task.exec params['sql-file']
      task.grant_if params['grant'], params['dest-table']
    }
  }
}
