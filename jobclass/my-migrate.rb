require 'bricolage/psqldatasource'

JobClass.define('my-migrate') {
  parameters {|params|
    # Export
    params.add SrcTableParam.new(optional: false)
    params.add DataSourceParam.new('mysql', 'src-ds', 'Source data source.')
    params.add DestFileParam.new('tmp-file', 'PATH', 'Temporary local file path.')
    params.add OptionalBoolParam.new('sqldump', 'If true, use sqldump command to dump, only on available.', default: true)

    # Put
    params.add DestFileParam.new('s3-file', 'PATH', 'Temporary S3 file path.')
    params.add DataSourceParam.new('s3', 's3-ds', 'Temporary file storage.')
    params.add OptionalBoolParam.new('override', 'If true, overwrite s3 target file.  Otherwise causes error.')

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
    params.add KeyValuePairsParam.new('grant', 'KEY:VALUE', 'GRANT table after SQL is executed.')

    # All
    params.add OptionalBoolParam.new('export', 'Runs EXPORT task.')
    params.add OptionalBoolParam.new('put', 'Runs PUT task.')
    params.add OptionalBoolParam.new('load', 'Runs LOAD task.')
    params.add OptionalBoolParam.new('gzip', 'If true, compresses target file by gzip.', default: true)
  }

  declarations {|params|
    decls = sql_statement(params).declarations
    decls.declare 'dest-table', nil
    decls
  }

  script {|params, script|
    run_all = !params['export'] && !params['put'] && !params['load']

    # Export
    if params['export'] || run_all
      script.task(params['src-ds']) {|task|
        task.export sql_statement(params),
          path: params['tmp-file'],
          format: 'json',
          override: true,
          gzip: params['gzip'],
          sqldump: params['sqldump']
      }
    end

    # Put
    if params['put'] || run_all
      script.task(params['s3-ds']) {|task|
        task.put params['tmp-file'], params['s3-file'], check_args: false
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
        task.load params['s3-ds'], params['s3-file'], work_table,
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
    srcs = params['src-tables']
    raise ParameterError, "src-tables must be singleton when no sql-file is given" unless srcs.size == 1
    src_table_var = srcs.keys.first
    stmt = SQLStatement.for_string("select * from $#{src_table_var};")
    stmt.declarations = Declarations.new({src_table_var => src_table_var})
    stmt
  end
}
