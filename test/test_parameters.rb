require 'test/unit'
require 'bricolage/parameters'
require 'optparse'
require 'pp'

module Bricolage
  class TestParameters < Test::Unit::TestCase
    def apply_values(decls, values)
      parser = Parameters::DirectValueHandler.new(decls)
      parser.parse(values)
    end

    def apply_options(decls, argv)
      parser = Parameters::CommandLineOptionHandler.new(decls)
      opts = OptionParser.new
      parser.define_options(opts)
      opts.parse!(argv)
      parser.values
    end

    def wrap_decl(decl)
      Parameters::Declarations.new.tap {|params|
        params.add decl
      }
    end

    default_context = nil
    default_variables = ResolvedVariables.new

    # StringParam (8)

    test "StringParam (*.job)" do
      decls = wrap_decl StringParam.new('options', 'OPTIONS', 'Loader options.', optional: true)
      pvals = apply_values(decls, {'options' => 'gzip, maxerror=3'})
      params = pvals.resolve(default_context, default_variables)
      assert_equal 'gzip, maxerror=3', params['options']
      assert_false params.variables.bound?('options')
    end

    test "StringParam (--opt)" do
      decls = wrap_decl StringParam.new('options', 'OPTIONS', 'Loader options.', optional: true)
      pvals = apply_options(decls, ['--options=gzip, maxerror=3'])
      params = pvals.resolve(default_context, default_variables)
      assert_equal 'gzip, maxerror=3', params['options']
      assert_false params.variables.bound?('options')
    end

    test "StringParam (default value)" do
      decls = wrap_decl StringParam.new('options', 'OPTIONS', 'Loader options.', optional: true)
      pvals = apply_values(decls, {})
      params = pvals.resolve(default_context, default_variables)
      assert_nil params['options']
    end

    test "StringParam (missing value)" do
      decls = wrap_decl StringParam.new('delete-cond', 'SQL_EXPR', 'DELETE condition.')
      pvals = apply_values(decls, {})
      assert_raise(ParameterError) {
        pvals.resolve(default_context, default_variables)
      }
    end

    # BoolParam (0)

    # OptionalBoolParam (41)

    test "OptionalBoolParam (*.job)" do
      decls = wrap_decl OptionalBoolParam.new('vacuum-sort', 'VACUUM SORT table after SQL is executed.')
      pvals = apply_values(decls, {'vacuum-sort' => true})
      params = pvals.resolve(default_context, default_variables)
      assert_true params['vacuum-sort']
      assert_false params.variables.bound?('vacuum_sort')
    end

    test "OptionalBoolParam (--opt)" do
      decls = wrap_decl OptionalBoolParam.new('vacuum-sort', 'VACUUM SORT table after SQL is executed.', publish: true)
      pvals = apply_options(decls, ['--vacuum-sort'])
      params = pvals.resolve(default_context, default_variables)
      assert_true params['vacuum-sort']
      assert_equal 'true', params.variables['vacuum_sort']
    end

    test "OptionalBoolParam (default value #1)" do
      decls = wrap_decl OptionalBoolParam.new('vacuum', 'VACUUM table after SQL is executed.')
      pvals = apply_values(decls, {})
      params = pvals.resolve(default_context, default_variables)
      assert_false params['vacuum']
      assert_false params.variables.bound?('vacuum')
    end

    test "OptionalBoolParam (default value #2)" do
      decls = wrap_decl OptionalBoolParam.new('gzip', 'If true, compresses target file by gzip.', default: true)
      pvals = apply_values(decls, {})
      params = pvals.resolve(default_context, default_variables)
      assert_true params['gzip']
      assert_false params.variables.bound?('gzip')
    end

    # DateParam (2)

    test "DateParam (*.job)" do
      decls = wrap_decl DateParam.new('to', 'DATE', 'End date of logs to delete (%Y-%m-%d).')
      pvals = apply_values(decls, {'to' => '2014-01-23'})
      params = pvals.resolve(default_context, default_variables)
      assert_equal Date.new(2014, 1, 23), params['to']
      assert_false params.variables.bound?('to')
    end

    test "DateParam (--opt)" do
      decls = wrap_decl DateParam.new('to', 'DATE', 'End date of logs to delete (%Y-%m-%d).', publish: true)
      pvals = apply_options(decls, ['--to=2014-01-23'])
      params = pvals.resolve(default_context, default_variables)
      assert_equal Date.new(2014, 1, 23), params['to']
      assert_equal '2014-01-23', params.variables['to']
    end

    test "DateParam (default value)" do
      decls = wrap_decl DateParam.new('to', 'DATE', 'End date of logs to delete (%Y-%m-%d).', optional: true)
      pvals = apply_values(decls, {})
      params = pvals.resolve(default_context, default_variables)
      assert_nil params['to']
      assert_false params.variables.bound?('to')
    end

    # EnumParam (4)

    test "EnumParam (*.job)" do
      decls = wrap_decl EnumParam.new('format', %w(tsv json), 'Data file format.', default: 'tsv')
      pvals = apply_values(decls, {'format' => 'json'})
      params = pvals.resolve(default_context, default_variables)
      assert_equal 'json', params['format']
      assert_false params.variables.bound?('format')
    end

    test "EnumParam (--opt)" do
      decls = wrap_decl EnumParam.new('format', %w(tsv json), 'Data file format.', default: nil, publish: true)
      pvals = apply_options(decls, ['--format=tsv'])
      params = pvals.resolve(default_context, default_variables)
      assert_equal 'tsv', params['format']
      assert_equal 'tsv', params.variables['format']
    end

    test "EnumParam (default value)" do
      decls = wrap_decl EnumParam.new('format', %w(tsv json), 'Data file format.', default: 'tsv')
      pvals = apply_values(decls, {})
      params = pvals.resolve(default_context, default_variables)
      assert_equal 'tsv', params['format']
    end

    # DataSourceParam (17)

    class DummyContext_ds
      def initialize(kind, name, result)
        @kind = kind
        @name = name
        @result = result
      end

      def get_data_source(kind, name)
        if kind == @kind and name == @name
          @result
        else
          raise ParameterError, "wrong ds argument: #{kind}, #{name}"
        end
      end
    end

    DummyDataSource = Struct.new(:name)
    app_ds = DummyDataSource.new('app')
    sql_ds = DummyDataSource.new('sql')

    test "DataSourceParam (*.job)" do
      decls = wrap_decl DataSourceParam.new('sql')
      pvals = apply_values(decls, {'data-source' => 'app'})
      ctx = DummyContext_ds.new('sql', app_ds.name, app_ds)
      params = pvals.resolve(ctx, default_variables)
      assert_equal app_ds, params['data-source']
      assert_false params.variables.bound?('data-source')
    end

    test "DataSourceParam (--opt)" do
      decls = wrap_decl DataSourceParam.new('sql')
      pvals = apply_options(decls, ['--data-source=app'])
      ctx = DummyContext_ds.new('sql', app_ds.name, app_ds)
      params = pvals.resolve(ctx, default_variables)
      assert_equal app_ds, params['data-source']
      assert_false params.variables.bound?('data-source')
    end

    test "DataSourceParam (default value)" do
      decls = wrap_decl DataSourceParam.new('sql')
      pvals = apply_values(decls, {})
      ctx = DummyContext_ds.new('sql', nil, sql_ds)
      params = pvals.resolve(ctx, default_variables)
      assert_equal sql_ds, params['data-source']
      assert_false params.variables.bound?('data-source')
    end

    # SQLFileParam (14)

    DummyContext_sqlfile = Struct.new(:name, :ext, :result)
    class DummyContext_sqlfile
      def parameter_file(_name, _ext)
        raise ParameterError, "bad argument: #{_name}, #{_ext}" unless [name, ext] == [_name, _ext]
        result
      end
    end

    dummy_sql_resource = StringResource.new("select * from t;")
    dummy_sql_file = SQLStatement.new(dummy_sql_resource)

    test "SQLFileParam (*.job)" do
      decls = wrap_decl SQLFileParam.new
      pvals = apply_values(decls, {'sql-file' => 'some_path.sql'})
      ctx = DummyContext_sqlfile.new('some_path.sql', 'sql', dummy_sql_resource)
      params = pvals.resolve(ctx, default_variables)
      assert_equal dummy_sql_file, params['sql-file']
      assert_false params.variables.bound?('sql_file')
    end

    test "SQLFileParam (--opt)" do
      decls = wrap_decl SQLFileParam.new
      pvals = apply_options(decls, ['--sql-file=some_path.sql'])
      ctx = DummyContext_sqlfile.new('some_path.sql', 'sql', dummy_sql_resource)
      params = pvals.resolve(ctx, default_variables)
      assert_equal dummy_sql_file, params['sql-file']
      assert_false params.variables.bound?('sql_file')
    end

    test "SQLFileParam (default value)" do
      decls = wrap_decl SQLFileParam.new(optional: true)
      pvals = apply_values(decls, {})
      params = pvals.resolve(default_context, default_variables)
      assert_nil params['sql-file']
    end

    # DestTableParam (9)

    test "DestTableParam (*.job)" do
      decls = wrap_decl DestTableParam.new
      pvals = apply_values(decls, {'dest-table' => 'schemaA.tableA'})
      params = pvals.resolve(default_context, default_variables)
      assert_equal TableSpec.new('schemaA', 'tableA'), params['dest-table']
      assert_equal 'schemaA.tableA', params.variables['dest_table']
    end

    test "DestTableParam (--opt)" do
      decls = wrap_decl DestTableParam.new(optional: false)
      pvals = apply_options(decls, ['--dest-table=schemaA.tableA'])
      params = pvals.resolve(default_context, default_variables)
      assert_equal TableSpec.new('schemaA', 'tableA'), params['dest-table']
      assert_equal 'schemaA.tableA', params.variables['dest_table']
    end

    test "DestTableParam (default value)" do
      decls = wrap_decl DestTableParam.new
      pvals = apply_values(decls, {})
      params = pvals.resolve(default_context, default_variables)
      assert_nil params['dest-table']
      assert_false params.variables.bound?('dest_table')
    end

    test "DestTableParam (variable expansion)" do
      decls = wrap_decl DestTableParam.new
      pvals = apply_values(decls, {'dest-table' => '$s.t'})
      vars = ResolvedVariables.new
      vars.add ResolvedVariable.new('s', 'SCH')
      params = pvals.resolve(default_context, vars)
      assert_equal TableSpec.new('SCH', 't'), params['dest-table']
      assert_equal 'SCH.t', params.variables['dest_table']
    end

    test "DestTableParam (no such variable)" do
      decls = wrap_decl DestTableParam.new
      pvals = apply_values(decls, {'dest-table' => '$s.t'})
      assert_raise(ParameterError) {
        pvals.resolve(default_context, default_variables)
      }
    end

    # SrcTableParam (9)

    test "SrcTableParam (*.job)" do
      decls = wrap_decl SrcTableParam.new
      pvals = apply_values(decls, {'src-tables' => {'a' => '$s.A', 'b' => 'B'}})
      vars = ResolvedVariables.new
      vars.add ResolvedVariable.new('s', 'SCH')
      params = pvals.resolve(default_context, vars)
      srcs = {'a' => TableSpec.new('SCH', 'A'), 'b' => TableSpec.new(nil, 'B')}
      assert_equal srcs, params['src-tables']
      assert_equal 'SCH.A', params.variables['a']
      assert_equal 'B', params.variables['b']
    end

    test "SrcTableParam (--opt)" do
      decls = wrap_decl SrcTableParam.new
      pvals = apply_options(decls, ['--src-table=a:A', '--src-table=b:B'])
      params = pvals.resolve(default_context, default_variables)
      srcs = {'a' => TableSpec.new(nil, 'A'), 'b' => TableSpec.new(nil, 'B')}
      assert_equal srcs, params['src-tables']
      assert_equal 'A', params.variables['a']
      assert_equal 'B', params.variables['b']
    end

    test "SrcTableParam (default value)" do
      decls = wrap_decl SrcTableParam.new
      pvals = apply_values(decls, {})
      params = pvals.resolve(default_context, default_variables)
      assert_equal({}, params['src-tables'])
      assert_false params.variables.bound?('a')
      assert_false params.variables.bound?('b')
    end

    # DestFileParam (7)

    test "DestFileParam (*.job)" do
      decls = wrap_decl DestFileParam.new
      pvals = apply_values(decls, {'dest-file' => '/some/path.txt'})
      params = pvals.resolve(default_context, default_variables)
      assert_equal Pathname.new('/some/path.txt'), params['dest-file']
    end

    test "DestFileParam (--opt)" do
      decls = wrap_decl DestFileParam.new
      pvals = apply_options(decls, ['--dest-file=/some/path.txt'])
      params = pvals.resolve(default_context, default_variables)
      assert_equal Pathname.new('/some/path.txt'), params['dest-file']
    end

    test "DestFileParam (no value error)" do
      decls = wrap_decl DestFileParam.new
      pvals = apply_values(decls, {})
      assert_raise(ParameterError) {
        pvals.resolve(default_context, default_variables)
      }
    end

    # SrcFileParam (2)

    test "SrcFileParam (*.job)" do
      decls = wrap_decl SrcFileParam.new
      pvals = apply_values(decls, {'src-file' => '/some/path.txt'})
      params = pvals.resolve(default_context, default_variables)
      assert_equal Pathname.new('/some/path.txt'), params['src-file']
    end

    test "SrcFileParam (--opt)" do
      decls = wrap_decl SrcFileParam.new
      pvals = apply_options(decls, ['--src-file=/some/path.txt'])
      params = pvals.resolve(default_context, default_variables)
      assert_equal Pathname.new('/some/path.txt'), params['src-file']
    end

    test "SrcFileParam (no value error)" do
      decls = wrap_decl SrcFileParam.new
      pvals = apply_values(decls, {})
      assert_raise(ParameterError) {
        pvals.resolve(default_context, default_variables)
      }
    end

    # KeyValuePairsParam (3)

    test "KeyValuePairsParam (*.job)" do
      decls = wrap_decl KeyValuePairsParam.new('grant', 'KEY:VALUE', 'GRANT table after SQL is executed.')
      pvals = apply_values(decls, {'grant' => {'on'=>'tbl', 'to'=>'$user'}})
      vars = ResolvedVariables.new
      vars.add ResolvedVariable.new('user', 'group gg')
      params = pvals.resolve(default_context, vars)
      assert_equal({'on'=>'tbl', 'to'=>'group gg'}, params['grant'])
    end

    test "KeyValuePairsParam (default value)" do
      decls = wrap_decl KeyValuePairsParam.new('grant', 'KEY:VALUE', 'GRANT table after SQL is executed.')
      pvals = apply_values(decls, {})
      params = pvals.resolve(default_context, default_variables)
      assert_nil params['grant']
    end

    # StringListParam (1)

    test "StringListParam (*.job)" do
      decls = wrap_decl StringListParam.new('args', 'ARG', 'Command line arguments.', publish: true)
      pvals = apply_values(decls, {'args' => ['a', '$basedir', 'c']})
      vars = ResolvedVariables.new
      vars.add ResolvedVariable.new('basedir', '/base/dir')
      params = pvals.resolve(default_context, vars)
      assert_equal ['a', '/base/dir', 'c'], params['args']
      assert_equal 'a /base/dir c', params.variables['args']
    end

    test "StringListParam (missing value)" do
      decls = wrap_decl StringListParam.new('args', 'ARG', 'Command line arguments.')
      pvals = apply_values(decls, {})
      assert_raise(ParameterError) {
        pvals.resolve(default_context, default_variables)
      }
    end

  end
end
