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
      if params['drop']
        # drop - create - load pattern
        unless params['table-def']
          raise ParameterError, "--table-def is required when --drop is enabled"
        end

        task.transaction {
          task.drop_force '${dest_table}'
          task.exec params['table-def']
          task.load params['src-ds'], params['src-file'], params['dest-table'],
              params['format'], params['jsonpath'], params['options']
          task.grant_if params['grant'], params['dest-table']
        }
        # ANALYZE, VACUUM is needless for newly loaded table, skip always.

      elsif params['truncate']
        # truncate - load pattern
        if params['drop'] or params['table-def']
          raise ParameterError, "--truncate and --drop/--table-def is exclusive"
        end

        task.truncate_if params['truncate']
        task.transaction {
          task.load params['src-ds'], params['src-file'], params['dest-table'],
              params['format'], params['jsonpath'], params['options']
          task.grant_if params['grant'], params['dest-table']
        }
        # ANALYZE, VACUUM is needless for newly loaded table, skip always.

      else
        # load only pattern

        task.transaction {
          task.load params['src-ds'], params['src-file'], params['dest-table'],
              params['format'], params['jsonpath'], params['options']
          task.grant_if params['grant'], params['dest-table']
          task.analyze_if params['analyze']
        }
        # We cannot execute VACUUM in transaction
        task.vacuum_if params['vacuum'], params['vacuum-sort']
      end
    }
  }
}
