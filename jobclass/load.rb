require 'bricolage/psqldatasource'

JobClass.define('load') {
  parameters {|params|
    params.add DataSourceParam.new('s3', 'src-ds', 'Input data source.')
    params.add SrcFileParam.new
    params.add DataSourceParam.new('sql', 'dest-ds', 'Target data source.')
    params.add DestTableParam.new(optional: false)
    params.add EnumParam.new('format', %w(tsv csv json), 'Data file format.', default: 'tsv')
    params.add StringParam.new('jsonpath', 'PATH', 'jsonpath to specify columns of json format records', optional: true)
    params.add KeyValuePairsParam.new('options', 'OPTIONS', 'Loader options.',
        optional: true, default: PSQLLoadOptions.new,
        value_handler: lambda {|value, ctx, vars| PSQLLoadOptions.parse(value) })
    params.add SQLFileParam.new('table-def', 'PATH', 'Create table file.', optional: true)
    params.add OptionalBoolParam.new('drop', 'DROP table before CREATE.')
    params.add OptionalBoolParam.new('truncate', 'TRUNCATE table before SQL is executed.')
    params.add OptionalBoolParam.new('analyze', 'ANALYZE table after SQL is executed.')
    params.add OptionalBoolParam.new('vacuum', 'VACUUM table after SQL is executed.')
    params.add OptionalBoolParam.new('vacuum-sort', 'VACUUM SORT table after SQL is executed.')
    params.add KeyValuePairsParam.new('grant', 'KEY:VALUE', 'GRANT table after SQL is executed. (required keys: privilege, to)')
  }

  declarations {|params|
    Declarations.new("dest_table" => nil)
  }

  script {|params, script|
    script.task(params['dest-ds']) {|task|
      task.drop_force_if params['drop']
      task.exec params['table-def'] if params['table-def']
      task.truncate_if params['truncate']
      task.load params['src-ds'], params['src-file'], params['dest-table'],
          params['format'], params['jsonpath'], params['options']
      task.vacuum_if params['vacuum'], params['vacuum-sort']
      task.analyze_if params['analyze']
      task.grant_if params['grant'], params['dest-table']
    }
  }
}
