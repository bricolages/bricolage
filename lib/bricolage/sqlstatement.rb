require 'bricolage/filesystem'
require 'bricolage/exception'

module Bricolage

  class SQLStatement
    def SQLStatement.delete_where(cond, location = nil)
      for_string("delete from $dest_table where #{cond};", location)
    end

    def SQLStatement.for_string(sql, location = nil)
      new(StringResource.new(sql, location))
    end

    def initialize(resource, declarations = nil)
      @resource = resource
      @declarations = declarations
      @code = nil
      @replace = nil
    end

    attr_reader :resource

    def ==(other)
      return false unless other.kind_of?(SQLStatement)
      @resource == other.resource
    end

    alias eql? ==

    def hash
      @resource.hash
    end

    def declarations
      @declarations ||= Declarations.parse(@resource)
    end

    attr_writer :declarations

    def replace(re, subst)
      @replace = [re, subst]
      self
    end

    def bind(ctx, vars)
      raise FatalError, "already bound SQL statement: #{@resource.name}" if @code
      src = ctx.parameter_file_loader.eruby(@resource.content, @resource.name || '-')
      @code = Variable.expand_string(apply_replace(src)) {|name|
        val = vars[name]
        val.respond_to?(:to_sql) ? val.to_sql : val.to_s
      }
    end

    def location
      @resource.name
    end

    def source
      @code or raise FatalError, "unbound SQL statement: #{@resource.name}"
    end

    def stripped_source
      strip_sql(source)
    end

    def kind
      # This implementation is not complete but useful
      first_word = @resource.content.gsub(%r<'(?:[^']+|'')*'|"(?:[^"]+|"")*"|--.*|/\*(?m:.*?)\*/>, '').strip.slice(/\A\w+/)
      op = first_word ? first_word.downcase : nil
      case op
      when 'with' then 'select'
      else op
      end
    end

    def inspect
      "\#<#{self.class} #{@resource.inspect}>"
    end

    def raw_content
      @resource.content
    end

    def stripped_raw_content
      strip_sql(raw_content)
    end

    def meta_data
      SQLMetaDataParser.new.parse(raw_content)
    end

    def attributes
      Array(meta_data['attributes'])
    end

    private

    def apply_replace(content)
      return content unless @replace
      re, subst = @replace
      content.gsub(re, subst)
    end

    def strip_sql(src)
      src.sub(%r{\A/\*.*?^\*/}m, '').gsub(/^--.*/, '').strip
    end
  end

  # Variable declarations in parameter files
  class Declarations
    def Declarations.parse(source)
      SQLMetaDataParser.new.parse_declarations(source)
    end

    def initialize(vars = {})
      @vars = {}
      vars.each do |name, default_value|
        declare name, default_value
      end
    end

    def inspect
      "\#<#{self.class} #{@vars.inspect}>"
    end

    def declared?(name)
      @vars.key?(name.to_s)
    end

    def [](name)
      @vars[name.to_s] or raise ParameterError, "undeclared variable: #{name}"
    end

    def each(&block)
      @vars.each_value(&block)
    end

    def default_variables
      Variables.define {|vars|
        each do |decl|
          if decl.have_default_value?
            vars[decl.name.to_s] = decl.default_value
          end
        end
      }
    end

    def freeze
      @vars.freeze
      super
    end

    def declare(name, value)
      raise ParameterError, "duplicated variable declaration: #{name}" if declared?(name)
      varname = name.to_s.tr('-', '_')
      @vars[varname] = Declaration.new(varname, value)
    end

    def declare_table(name, value)
      case value
      when String
        declare name, value
      when nil
        declare name, name
      else
        raise ParameterError, "bad default value for table variable: #{value.class} (#{value.inspect})"
      end
    end

    def declare_tables(label, value)
      case value
      when String
        declare value, value
      when Array
        value.each do |name|
          declare name, name
        end
      when Hash
        value.each do |name, val|
          declare name, val
        end
      else
        raise ParameterError, "'#{label}' entry value must be a string, a list or a map but a #{value.class}"
      end
    end

    def declare_map(label, value)
      case value
      when String
        declare value, nil
      when Array
        value.each do |name|
          declare name, nil
        end
      when Hash
        value.each do |name, val|
          declare name, val
        end
      else
        raise ParameterError, "'#{label}' entry value must be a string, a list or a map but a #{value.class}"
      end
    end

    def declare_none(label, value)
      # ignore
    end
  end

  # Accepts all variable as declared dynamically
  class DynamicDeclarations
    def declared?(name)
      true
    end

    def each
    end

    def [](name)
      Declaration.new(name.to_s, nil)
    end
  end

  class Declaration
    def initialize(name, default_value)
      @name = name
      @default_value = default_value
    end

    attr_reader :name
    attr_reader :default_value

    def have_default_value?
      not @default_value.nil?
    end

    def inspect
      "\#<#{self.class} #{@name}#{have_default_value? ? '=' + @default_value.inspect : ''}>"
    end
  end

  class SQLMetaDataParser
    def parse_declarations(source)
      build_decls(parse(source))
    end

    def parse(source)
      parse_meta(read_mata(source))
    end

    private

    DECLARATIONS = {
      'dest-table' => 'table',
      'src-tables' => 'tables',
      'params' => 'map',
      'attributes' => 'none'
    }

    def build_decls(map)
      decls = Declarations.new
      DECLARATIONS.each do |key, type|
        value = map.delete(key)
        decls.send "declare_#{type}", key, value if value
      end
      unless map.empty?
        raise ParameterError, "unknown SQL parameter declaration: #{map.keys.join(', ')}"
      end
      decls.freeze
      decls
    end

    def parse_meta(source)
      return Hash.new if source.empty?
      YAML.load(source) || {}
    rescue YAML::SyntaxError => ex
      raise ParameterError, "SQL meta data syntax error: #{ex.message}"
    end

    KEYS_RE = /\A--(?:#{DECLARATIONS.keys.join('|')}):/

    def read_mata(source)
      lines = ''
      source.each_line do |line|
        case line
        when KEYS_RE
          lines.concat line.sub(/\A--/, '')
        when /\A--([\w\-]+):/
          key = $1
          raise ParameterError, "unknown SQL meta data: #{source.name}: #{key}"
        when /\A--/
          # skip non-metadata comments
          lines.concat "\n"
        else
          break
        end
      end
      lines
    end
  end

  class TableSpec
    # "[SCHEMA.]TABLE" -> TableSpec(SCHEMA, TABLE)
    def TableSpec.parse(spec)
      new(*split_name(spec))
    end

    # "TABLE" -> [nil, "TABLE"]
    # "SCHEMA.TABLE" -> ["SCHEMA", "TABLE"]
    def TableSpec.split_name(name_pair)
      raise ParameterError, "table spec is empty" if name_pair.strip.empty?
      components = name_pair.split('.', 2)
      if components.size == 1
        return nil, components.first
      else
        s, t = components
        raise ParameterError, "schema name is blank" if not s or s.empty?
        raise ParameterError, "table name is blank" if not t or t.empty?
        return s, t
      end
    end

    def initialize(schema, name)
      @schema = schema
      @name = name
    end

    attr_reader :schema
    attr_reader :name

    def to_s
      @schema ? "#{@schema}.#{@name}" : @name
    end

    def inspect
      "\#<#{self.class} #{to_s}>"
    end

    def ==(other)
      return false unless other.kind_of?(TableSpec)
      @schema == other.schema and @name == other.name
    end

    alias eql? ==

    def hash
      @schema.hash ^ @name.hash
    end
  end

end
