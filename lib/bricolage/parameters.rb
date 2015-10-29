require 'bricolage/variables'
require 'bricolage/sqlstatement'
require 'bricolage/exception'
require 'date'
require 'pathname'

module Bricolage

  class Parameters

    # Job parameter declarations defined by job class.
    class Declarations
      def initialize
        @decls = {}   # {name => Param}
      end

      def inspect
        "\#<#{self.class} #{@decls.inspect}>"
      end

      def add(param)
        @decls[param.name] = param
      end

      def [](key)
        @decls[key]
      end

      def keys
        @decls.keys
      end

      def each(&block)
        @decls.each_value(&block)
      end

      def parse_default_values(values)
        return IntermediateValues.empty(self) unless values
        DefaultValuesHandler.new(self).parse(values)
      end

      def parse_direct_values(values)
        DirectValueHandler.new(self).parse(values)
      end

      def parsing_options
        h = CommandLineOptionHandler.new(self)
        yield h
        h.values
      end

      def union_intermediate_values(*ival_list)
        IntermediateValues.union(self, *ival_list)
      end
    end

    # Handles default values given by variable.yml (global or subsystem variables)
    # Declarations + values -> IntermediateValues
    class DefaultValuesHandler
      def initialize(decls)
        @decls = decls
      end

      def parse(values)
        unless values.kind_of?(Hash)
          raise ParameterError, "invalid type for 'defaults' global variable: #{values.class}"
        end
        parsed_values = {}
        values.each do |name, value|
          decl = @decls[name]
          next unless decl   # ignore undeclared option
          val = decl.parse_value(value)
          # nil means really nil for default values.
          parsed_values[name] = val
        end
        IntermediateValues.new(@decls, parsed_values, Variables.new)
      end
    end

    # Handles *.job file values.
    # Declarations + values -> IntermediateValues
    class DirectValueHandler
      def initialize(decls)
        @decls = decls   # Declarations
      end

      # values :: {String => a}
      def parse(values)
        parsed_values = {}
        vars = Variables.new
        values.each do |name, value|
          if decl = @decls[name]
            val = decl.parse_value(value)
            # nil is equal to "no option given" semantically
            parsed_values[name] = val unless val.nil?
          else
            vars.add Variable.new(name, value)
          end
        end
        IntermediateValues.new(@decls, parsed_values, vars)
      end
    end

    # Handles param values given by command line options (e.g. --dest-table=t).
    # Declarations + option_args -> IntermediateValues
    class CommandLineOptionHandler
      def initialize(decls)
        @decls = decls   # Declarations
        @values = {}     # {name => a}
        @vars = Variables.new
      end

      def define_options(parser)
        @decls.each do |decl|
          desc = (decl.optional? ? '[optional] ' : '') + decl.description
          arg_spec = decl.have_arg? ? "=#{decl.arg_spec}" : ''
          parser.on("--#{decl.option_name}#{arg_spec}", desc) {|arg|
            @values[decl.name] = decl.parse_option_value(arg, @values[decl.name])
          }
        end
        parser.on('-v', '--variable=NAME=VALUE', 'Set variable.') {|name_value|
          name, value = name_value.split('=', 2)
          @vars.add Variable.new(name, value)
        }
      end

      def values
        IntermediateValues.new(@decls, @values, @vars)
      end
    end

    # Unified intermediate param values representation.
    # This class is used for both of *.job file and options.
    class IntermediateValues
      def IntermediateValues.union(decls, *vals_list)
        result = empty(decls)
        vals_list.each do |vals|
          result.update vals
        end
        result
      end

      def IntermediateValues.empty(decls)
        new(decls, {}, Variables.new)
      end

      def initialize(decls, values, vars)
        @decls = decls     # Declarations
        @values = values   # {name => a}
        @variables = vars  # Variables
      end

      attr_reader :decls
      attr_reader :values
      attr_reader :variables

      def [](name)
        @values[name]
      end

      def []=(name, value)
        @values[name] = value
      end

      def keys
        @values.keys
      end

      def update(other)
        unless @decls == other.decls
          raise "[BUG] merging IntermediateValues with different paramdecl: #{self.inspect} - #{other.inspect}"
        end
        @values.update other.values
        @variables.update other.variables
      end

      def resolve(ctx, vars)
        materialized = materialize(ctx, vars)
        resolved = resolved_variables(materialized, vars)
        Parameters.new(@decls, materialized, resolved, ctx)
      end

      private

      def materialize(ctx, vars)
        h = {}
        @decls.each do |decl|
          value = @values[decl.name]
          # value==nil means "no parameter given" or "no option given".
          # Note that false is a *valid* value, "falthy" check is not sufficient here.
          if value.nil?
            raise ParameterError, "parameter not given: #{decl.name}" if decl.required?
            h[decl.name] = decl.default_value(ctx, vars)
          else
            h[decl.name] = decl.materialize(value, ctx, vars)
          end
        end
        h
      end

      def resolved_variables(materialized, base_vars)
        Variables.define {|vars|
          materialized.each do |name, value|
            decl = @decls[name]
            next unless decl.publish?
            next if value.nil?
            decl.variables(value).each do |var|
              vars.add var
            end
          end
          vars.update @variables
        }.resolve_with(base_vars)
      end
    end

    # class Parameters
    # Materialized, fixed job parameter values.

    def initialize(decls, values, vars, ctx)
      @decls = decls     # Declarations
      @values = values   # {name => a}
      @variables = vars  # ResolvedVariables
      @context = ctx     # Context
    end

    def inspect
      "\#<#{self.class} #{@values.inspect}>"
    end

    def [](key)
      raise ParameterError, "no such parameter: #{key}" unless @values.key?(key)
      @values[key]
    end

    alias get []

    def keys
      @values.keys
    end

    attr_reader :variables

    # FIXME: remove
    def generic_ds
      @context.get_data_source('generic', 'generic')
    end

    # FIXME: remove
    def file_ds
      @context.get_data_source('file', 'file')
    end

    # FIXME: remove
    def ruby_ds
      @context.get_data_source('ruby', 'ruby')
    end

  end   # class Parameters

  class Param
    def initialize(name, arg_spec, description, optional: false, publish: false)
      @name = name
      @arg_spec = arg_spec
      @description = description
      @optional = optional
      @publish = publish
    end

    attr_reader :name
    attr_reader :description

    def option_name
      name
    end

    attr_reader :arg_spec

    def have_arg?
      !!@arg_spec
    end

    def optional?
      @optional
    end

    def required?
      not optional?
    end

    # "published" parameter defines SQL variable.
    def publish?
      @publish
    end

    def inspect
      attrs = [
        (@optional ? 'optional' : 'required'),
        (@publish ? 'publish' : nil)
      ].compact.join(',')
      "\#<#{self.class} #{name} #{attrs}>"
    end

    #
    # Value Handling
    #

    def parse_option_value(arg, acc)
      return nil if arg.nil?
      arg
    end

    def parse_value(value)
      value
    end

    # abstract def default_value(ctx, vars)
    # abstract def materialize(value, ctx, vars)
    # abstract def variables(value)

    private

    def expand(str, vars)
      Variable.expand_string(str.to_s) {|var|
        vars[var] or raise ParameterError, "undefined variable in parameter #{name}: #{var}"
      }
    end

    def wrap_variable_value(val)
      [ResolvedVariable.new(name.gsub('-', '_'), val)]
    end
  end

  class StringParam < Param
    def initialize(name, arg_spec, description, optional: false, publish: false)
      super name, arg_spec, description, optional: optional, publish: publish
    end

    def default_value(ctx, vars)
      nil
    end

    def materialize(value, ctx, vars)
      expand(value, vars)
    end

    def variables(value)
      wrap_variable_value(value)
    end
  end

  class BoolParam < Param
    def initialize(name, description, publish: false)
      super name, nil, description, publish: publish
    end

    def default_value(ctx, vars)
      false
    end

    def materialize(value, ctx, vars)
      !!value
    end

    def variables(bool)
      wrap_variable_value(bool.to_s)
    end
  end

  class OptionalBoolParam < Param
    def initialize(name, description, default: false, publish: false)
      super name, nil, description, optional: true, publish: publish
      @default_value = default
    end

    def default_value(ctx, vars)
      @default_value
    end

    def materialize(value, ctx, vars)
      !!value
    end

    def variables(bool)
      wrap_variable_value(bool.to_s)
    end
  end

  class DateParam < Param
    def initialize(name, arg_spec, description, optional: false, publish: false)
      super name, arg_spec, description, optional: optional, publish: publish
    end

    def default_value(ctx, vars)
      nil
    end

    def materialize(value, ctx, vars)
      case value
      when Date
        value
      when String
        begin
          Date.parse(expand(value, vars))
        rescue ArgumentError
          raise ParameterError, "bad date format: #{value.inspect}"
        end
      else
        raise ParameterError, "unknown type for date parameter '#{name}': #{value.class}"
      end
    end

    def variables(date)
      # "YYYY-MM-DD"
      wrap_variable_value(date.to_s)
    end
  end

  class EnumParam < Param
    def initialize(name, list, description, default: nil, publish: false)
      super name, 'VALUE', description, optional: (default ? true : false), publish: publish
      @list = list.map {|val| val.to_s.freeze }
      @default_value = default
    end

    def description
      "#{super} (#{@list.join(', ')})" + (@default ? " [default: #{@default}]" : '')
    end

    def default_value(ctx, vars)
      @default_value
    end

    def materialize(value, ctx, vars)
      val = expand(value.to_s, vars)
      unless @list.include?(val)
        raise ParameterError, "unknown value for enum parameter '#{name}': #{val.inspect}"
      end
      val
    end

    def variables(val)
      wrap_variable_value(val)
    end
  end

  class DataSourceParam < Param
    def initialize(kind, name = 'data-source', description = 'Main data source.', optional: true, publish: false)
      raise FatalError, "no data source kind declared" unless kind
      super name, 'NAME', description, optional: optional, publish: publish
      @kind = kind
    end

    def description
      "#{super} [default: #{@kind}]"
    end

    def default_value(ctx, vars)
      ctx.get_data_source(@kind, nil)
    end

    def materialize(value, ctx, vars)
      ctx.get_data_source(@kind, expand(value, vars))
    end

    def variables(ds)
      wrap_variable_value(ds.name)
    end
  end

  class SQLFileParam < Param
    def initialize(name = 'sql-file', arg_spec = 'PATH', description = 'SQL file.', optional: false, publish: false)
      super name, arg_spec, description, optional: optional, publish: publish
    end

    def default_value(ctx, vars)
      nil
    end

    def materialize(name_or_stmt, ctx, vars)
      case name_or_stmt
      when String
        name = name_or_stmt
        SQLStatement.new(ctx.parameter_file(expand(name.to_s, vars), 'sql'))
      when SQLStatement
        name_or_stmt
      else
        raise ParameterError, "unknown type for parameter #{name}: #{name_or_stmt.class}"
      end
    end

    def variables(stmt)
      wrap_variable_value(stmt.location)
    end
  end

  class DestTableParam < Param
    def initialize(
        name = 'dest-table',
        arg_spec = '[SCHEMA.]TABLE',
        description = 'Target table name.',
        # [CLUDGE] Default dest_table is provided by SQL parameter declarations,
        # we cannot require the value here.  I know this is bad...
        optional: true,
        publish: true
    )
      super name, arg_spec, description, optional: optional, publish: publish
    end

    def default_value(ctx, vars)
      nil
    end

    def materialize(spec, ctx, vars)
      TableSpec.parse(expand(spec, vars))
    end

    def variables(spec)
      wrap_variable_value(spec.to_s)
    end
  end

  class SrcTableParam < Param
    def initialize(
        name = 'src-tables',
        arg_spec = 'VAR:[SCHEMA.]TABLE',
        description = 'Source table name.',
        optional: true,   # Source tables may not exist (e.g. data loading)
        publish: true
    )
      super name, arg_spec, description, optional: optional, publish: publish
    end

    def option_name
      'src-table'
    end

    def parse_option_value(value, h)
      var, spec = value.split(':', 2)
      if not var or var.empty?
        raise ParameterError, "missing variable name: #{value.inspect}"
      end
      (h ||= {})[var] = spec
      h   # accumulator
    end

    def parse_value(h)
      raise ParameterError, "bad type for parameter #{name}: #{h.class}" unless h.kind_of?(Hash)
      h.empty? ? nil : h
    end

    def default_value(ctx, vars)
      {}
    end

    def materialize(h, ctx, vars)
      map = {}
      h.each do |name, spec|
        map[name] = TableSpec.parse(expand(spec, vars))
      end
      map
    end

    def variables(h)
      h.map {|name, value| ResolvedVariable.new(name, value.to_s) }
    end
  end

  class DestFileParam < Param
    def initialize(name = 'dest-file', arg_spec = 'PATH', description = 'Target file name.',
        optional: false, publish: false)
      super name, arg_spec, description, optional: optional, publish: publish
    end

    def default_value(ctx, vars)
      nil
    end

    def materialize(path, ctx, vars)
      Pathname(expand(path, vars))
    end

    def variables(path)
      wrap_variable_value(path.to_s)
    end
  end

  class SrcFileParam < Param
    def initialize(name = 'src-file', arg_spec = 'PATH', description = 'Source file name.',
        optional: false, publish: false)
      super name, arg_spec, description, optional: optional, publish: publish
    end

    def default_value(ctx, vars)
      nil
    end

    def materialize(path, ctx, vars)
      Pathname(expand(path, vars))
    end

    def variables(path)
      wrap_variable_value(path.to_s)
    end
  end

  class StringListParam < Param
    def initialize(name, arg_spec, description, optional: false, publish: false)
      super name, arg_spec, description, optional: optional, publish: publish
    end

    def parse_option_value(value, list)
      (list ||= []).push value
      list   # accumulator
    end

    def parse_value(vals)
      raise ParameterError, "bad type for parameter #{name}: #{vals.class}" unless vals.kind_of?(Array)
      vals.empty? ? nil : vals
    end

    def default_value(ctx, vars)
      []
    end

    def materialize(vals, ctx, vars)
      vals.map {|val| expand(val, vars) }
    end

    def variables(strs)
      wrap_variable_value(strs.join(' '))
    end
  end

  class KeyValuePairsParam < Param
    def initialize(name, arg_spec, description, optional: true, default: nil, value_handler: nil)
      super name, arg_spec, description, optional: optional, publish: false
      @default_value = default
      @value_handler = value_handler
    end

    def parse_option_value(value, h)
      var, spec = value.split(':', 2)
      if not var or var.empty?
        raise ParameterError, "missing variable name: #{value.inspect}"
      end
      (h ||= {})[var] = spec
      h   # accumulator
    end

    def parse_value(h)
      case h
      when Hash
        h.empty? ? nil : h
      when String   # FIXME: should be removed after changing all load options
        raise ParameterError, "bad type for parameter #{name}: #{h.class}" unless @value_handler
        h.strip.empty? ? nil : h
      when nil, false   # disables option explicitly
        {}
      else
        raise ParameterError, "bad type for parameter #{name}: #{h.class}"
      end
    end

    def default_value(ctx, vars)
      @default_value
    end

    def materialize(pairs, ctx, vars)
      if @value_handler
        @value_handler.call(pairs, ctx, vars)
      else
        unless pairs.kind_of?(Hash)
          raise "[BUG] bad value type #{pairs.class} for KeyValuePairsParam\#materialize (#{name})"
        end
        h = {}
        pairs.each do |name, value|
          h[name] = expand(value.to_s, vars)
        end
        h
      end
    end

    def variables(h)
      []
    end
  end

end
