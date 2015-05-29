JobClass.define('my-export') {
  parameters {|params|
    params.add SQLFileParam.new(optional: true)
    params.add DestFileParam.new
    params.add SrcTableParam.new
    params.add EnumParam.new('format', %w(json tsv csv), 'Target file format.', default: 'json')
    params.add OptionalBoolParam.new('gzip', 'If true, compresses target file by gzip.')
    params.add OptionalBoolParam.new('override', 'If true, clears target file.  Otherwise causes error.')
    params.add OptionalBoolParam.new('sqldump', 'If true, clears use sqldump command to dump, only wheen usable.')
    params.add DataSourceParam.new('mysql')
  }

  declarations {|params|
    sql_statement(params).declarations
  }

  script {|params, script|
    script.task(params['data-source']) {|task|
      task.export sql_statement(params),
        path: params['dest-file'],
        format: params['format'],
        override: params['override'],
        gzip: params['gzip'],
        sqldump: params['sqldump']
    }
  }

  def sql_statement(params)
    if sql = params['sql-file']
      sql
    else
      srcs = params['src-tables']
      raise ParameterError, "src-tables must be singleton when no sql-file is given" unless srcs.size == 1
      src_table_var = srcs.keys.first
      stmt = SQLStatement.for_string("select * from $#{src_table_var};")
      stmt.declarations = Declarations.new({src_table_var => src_table_var})
      stmt
    end
  end
}
