require 'bricolage/exception'
require 'pathname'

module Bricolage

  class JobFile
    def JobFile.load(ctx, path)
      values = if /\.sql\.job\z/ =~ path.to_s
        load_embedded_definition(ctx, path)
      else
        ctx.parameter_file_loader.load_yaml(path)
      end
      parse(values, path)
    end

    class << JobFile
      private

      def load_embedded_definition(ctx, path)
        sql = ctx.parameter_file_loader.load_text(path)
        block = sql.slice(%r{\A/\*.*?^\*/}m) or
            raise ParameterError, "missing embedded job definition block: #{path}"
        yaml = block.sub(%r{\A/\*}, '').sub(%r{^\*/\s*\z}, '')
        begin
          values = YAML.load(yaml)
        rescue => err
          raise ParameterError, "#{path}: embedded job definition syntax error: #{err.message}"
        end
        # avoid changing line number
        stripped_sql = sql.sub(%r{\A/\*.*?^\*/}m, "\n" * block.count("\n"))
        decls = make_sql_declarations(stripped_sql, values, path)
        stmt = SQLStatement.new(StringResource.new(sql, path), decls)
        set_value values, 'sql-file', stmt, path
        values
      end

      def set_value(values, name, value, path)
        raise ParameterError, "#{path}: #{name} parameter and embedded SQL script is exclusive" if values[name]
        values[name] = value
      end

      def make_sql_declarations(sql, values, path)
        decls = Declarations.new
        vars = Variable.list(sql)
        if dest = vars.delete('dest_table')
          decls.declare 'dest_table', nil
        end
        if values['src-tables']
          srcs = values['src-tables']
          case srcs
          when String
            decls.declare srcs, nil
            vars.delete srcs
          when Array
            srcs.each do |table|
              decls.declare table, nil
              vars.delete table
            end
          when Hash
            srcs.each_key do |table|
              decls.declare table, nil
              vars.delete table
            end
          else
            raise ParameterError, "unknown src-tables value type: #{srcs.class}"
          end
        end
        vars.each do |name|
          decls.declare name, name
        end
        decls
      end
    end

    def JobFile.parse(values, path)
      values = values.dup
      class_id = values.delete('class') or
          raise ParameterError, "missing job class: #{path}"
      new(class_id, values, path)
    end

    def initialize(class_id, values, path)
      @class_id = class_id
      @values = values
      @path = Pathname(path)
    end

    attr_reader :class_id
    attr_reader :values
    attr_reader :path

    def job_id
      base = @path.basename('.job').to_s
      File.basename(base, '.*')
    end

    def subsystem
      @path.parent.basename.to_s
    end
  end

end
