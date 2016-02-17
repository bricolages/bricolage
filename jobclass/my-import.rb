require 'bricolage/psqldatasource'
require 'bricolage/mysqldatasource'

JobClass.define('my-import') {
  parameters {|params|
    # S3Export
    params.add SrcTableParam.new(optional: false)
    params.add DataSourceParam.new('mysql', 'src-ds', 'Source data source.')
    params.add SQLFileParam.new(optional: true)
    params.add DataSourceParam.new('s3', 's3-ds', 'Temporary file storage.')
    params.add DestFileParam.new('s3-prefix', 'PREFIX', 'Temporary S3 prefix.')
    params.add KeyValuePairsParam.new('dump-options', 'KEY:VALUE', 'dump options.', optional: true)

    # Load
    params.add DestTableParam.new(optional: false)
    params.add DataSourceParam.new('sql', 'dest-ds', 'Destination data source.')
    params.add KeyValuePairsParam.new('options', 'OPTIONS', 'Loader options.',
        optional: true, default: PSQLLoadOptions.new,
        value_handler: lambda {|value, ctx, vars| PSQLLoadOptions.parse(value) })
    params.add SQLFileParam.new('table-def', 'PATH', 'Create table file.')

    # Misc
    params.add OptionalBoolParam.new('analyze', 'ANALYZE table after SQL is executed.', default: true)
    params.add OptionalBoolParam.new('vacuum', 'VACUUM table after SQL is executed.')
    params.add OptionalBoolParam.new('vacuum-sort', 'VACUUM SORT table after SQL is executed.')
    params.add KeyValuePairsParam.new('grant', 'KEY:VALUE', 'GRANT table after SQL is executed. (required keys: privilege, to)')

    # All
    params.add OptionalBoolParam.new('export', 'Runs EXPORT task.')
    params.add OptionalBoolParam.new('put', 'Runs PUT task.')
    params.add OptionalBoolParam.new('load', 'Runs LOAD task.')
    params.add OptionalBoolParam.new('gzip', 'Compress Temporary files.')
  }

  declarations {|params|
    decls = sql_statement(params).declarations
    decls.declare 'dest-table', nil
    decls
  }

  script {|params, script|
    run_all = !params['export'] && !params['put'] && !params['load']

    # S3Export
    if params['export'] || run_all
      script.task(params['src-ds']) {|task|
        task.s3export params['src-tables'].keys.first,
                      sql_statement(params),
                      params['s3-ds'],
                      params['s3-prefix'],
                      params['gzip'],
                      dump_options: params['dump-options']
      }
    end

    # Load
    if params['load'] || run_all
      script.task(params['dest-ds']) {|task|
        prev_table = '${dest_table}_old'
        work_table = '${dest_table}_wk'

        # CREATE
        task.drop_force prev_table
        task.drop_force work_table
        task.exec params['table-def'].replace(/\$\{?dest_table\}?\b/, work_table)

        # COPY
        task.load params['s3-ds'], params['s3-prefix'], work_table,
            'json', nil, params['options'].merge('gzip' => params['gzip'])

        # VACUUM, ANALYZE, GRANT
        task.vacuum_if params['vacuum'], params['vacuum-sort'], work_table
        task.analyze_if params['analyze'], work_table
        task.grant_if params['grant'], work_table

        # RENAME
        task.create_dummy_table '${dest_table}'
        task.transaction {
          task.rename_table params['dest-table'].to_s, "#{params['dest-table'].name}_old"
          task.rename_table work_table, params['dest-table'].name
        }
      }
    end
  }

  def sql_statement(params)
    return params['sql-file'] if params['sql-file']
    srcs = params['src-tables']
    raise ParameterError, "src-tables must be singleton when no sql-file is given" unless srcs.size == 1
    src_table_var = srcs.keys.first
    query = add_place_holder(params, "select * from $#{src_table_var}")
    stmt = SQLStatement.for_string(query)
    stmt.declarations = Declarations.new({src_table_var => src_table_var})
    stmt
  end

  def add_place_holder(params, query)
    if params['dump-options']['partition_column']
      return query + " WHERE @PARTITION_CONDITION@;"
    else
      return query + ";"
    end
  end
}
