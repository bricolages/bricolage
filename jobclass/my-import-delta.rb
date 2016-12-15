require 'bricolage/psqldatasource'
require 'bricolage/mysqldatasource'

JobClass.define('my-import-delta') {
  parameters {|params|
    # S3Export
    params.add SrcTableParam.new(optional: false)
    params.add DataSourceParam.new('mysql', 'src-ds', 'Source data source.')
    params.add SQLFileParam.new(optional: true)
    params.add DataSourceParam.new('s3', 's3-ds', 'Temporary file storage.')
    params.add DestFileParam.new('s3-prefix', 'PREFIX', 'Temporary S3 prefix.')
    params.add KeyValuePairsParam.new('dump-options', 'KEY:VALUE', 'dump options.', optional: true)

    # Delete, Load
    params.add DataSourceParam.new('sql', 'dest-ds', 'Destination data source.')
    params.add StringParam.new('delete-cond', 'SQL_EXPR', 'DELETE condition.')
    params.add DestTableParam.new(optional: false)
    params.add KeyValuePairsParam.new('options', 'OPTIONS', 'Loader options.',
        optional: true, default: PSQLLoadOptions.new,
        value_handler: lambda {|value, ctx, vars| PSQLLoadOptions.parse(value) })

    # Misc
    params.add OptionalBoolParam.new('analyze', 'ANALYZE table after SQL is executed.', default: true)
    params.add OptionalBoolParam.new('vacuum', 'VACUUM table after SQL is executed.')
    params.add OptionalBoolParam.new('vacuum-sort', 'VACUUM SORT table after SQL is executed.')

    # All
    params.add OptionalBoolParam.new('export', 'Runs EXPORT task.')
    params.add OptionalBoolParam.new('load', 'Runs LOAD task.')
    params.add OptionalBoolParam.new('gzip', 'Compress Temporary files.')
  }

  script {|params, script|
    run_all = !params['export'] && !params['load']

    # S3Export
    if params['export'] || run_all
      script.task(params['src-ds']) {|task|
        task.s3export params['src-tables'].values.first.to_s,
                      params['sql-file'],
                      params['s3-ds'],
                      params['s3-prefix'],
                      params['gzip'],
                      dump_options: params['dump-options']
      }
    end

    # Load
    if params['load'] || run_all
      script.task(params['dest-ds']) {|task|
        task.transaction {
          # DELETE
          task.exec SQLStatement.delete_where(params['delete-cond']) if params['delete-cond']

          # COPY
          task.load params['s3-ds'], params['s3-prefix'], params['dest-table'],
              'json', nil, params['options'].merge('gzip' => params['gzip'])
        }

        # VACUUM, ANALYZE
        task.vacuum_if params['vacuum'], params['vacuum-sort'], params['dest-table']
        task.analyze_if params['analyze'], params['dest-table']
      }
    end
  }
}
