require 'bricolage/psqldatasource'
require 'bricolage/redisdatasource'
require 'redis'

JobClass.define('redis-export') {
  parameters {|params|
    # Export
    params.add DataSourceParam.new('psql', 'src-ds', 'Source data source.')
    params.add SrcTableParam.new(optional: false)
    params.add SQLFileParam.new(optional: true)

    # Redis import
    params.add DataSourceParam.new('redis', 'dest-ds', 'Redis cluster')
    params.add StringParam.new('key-column', 'REDIS_KEY', 'Redis object key. default: id', optional: true)
    params.add StringParam.new('prefix', 'REDIS_PREFIX', 'Redis object key prefix', optional: true)
    params.add StringParam.new('encode', 'REDIS_ENCODE', 'Redis object encoding. default: hash', optional: true)
  }

  script {|params, script|
    # Export
      script.task(params['dest-ds']) {|task|
        task.import params['src-ds'],
          params['src-tables'].first,
          sql_statement(params),
          params['key-column'] || "id",
          params['prefix'],
          params['encode'] || "hash"
      }
  }

  def sql_statement(params)
    return params['sql-file'] if params['sql-file']
    srcs = params['src-tables']
    raise ParameterError, "src-tables must be singleton when no sql-file is given" unless srcs.size == 1
    src_table_var = srcs.keys.first
    stmt = SQLStatement.for_string("select * from $#{src_table_var};")
    stmt.declarations = Declarations.new({src_table_var => src_table_var})
    stmt
  end
}
