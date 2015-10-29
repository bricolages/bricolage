require 'bricolage/exception'

module Bricolage

  class Variables
    def Variables.union(*vars_list)
      new.tap {|result|
        vars_list.each do |vars|
          result.update vars
        end
      }
    end

    def Variables.define
      new.tap {|vars|
        yield vars
      }
    end

    def initialize
      @vars = {}
    end

    def inspect
      "\#<#{self.class} #{@vars.inspect}>"
    end

    def resolved?
      false
    end

    def each_variable(&block)
      @vars.each_value(&block)
    end

    def [](name)
      var = @vars[name] or raise ParameterError, "no such variable: #{name}"
      var.value
    end

    def get_force(name)
      var = @vars[name]
      var ? var.value : nil
    end

    def []=(name, value)
      add Variable.new(name, value)
    end

    def keys
      @vars.keys
    end

    def add(var)
      @vars[var.name] = var
    end

    def update(vars)
      vars.each_variable do |var|
        add var
      end
    end

    # resolve self recursively
    def resolve
      ResolvedVariables.define {|resolved|
        @vars.each_value do |var|
          resolved.add do_expand_variable(var, resolved, {})
        end
      }
    end

    def do_expand_variable(var, resolved, seen)
      if seen[var.name]
        cycle = (seen.keys + [var.name]).join(', ')
        raise ParameterError, "recursive variable reference: #{var.name} (#{cycle})"
      end
      seen[var.name] = true
      if var.resolved?
        var
      else
        value = Variable.expand_string(var.value.to_s) {|name|
          if resolved.bound?(name)
            resolved[name]
          elsif var2 = @vars[name]
            res_var2 = do_expand_variable(var2, resolved, seen)
            resolved.add res_var2
            res_var2.value
          else
            raise ParameterError, "undefined variable in parameter #{var.name}: $#{name}"
          end
        }
        ResolvedVariable.new(var.name, value)
      end
    end
    private :do_expand_variable

    def resolve_with(resolved)
      raise "[BUG] unresolved variables given" unless resolved.resolved?
      ResolvedVariables.define {|result|
        each_variable do |var|
          if var.resolved?
            result.add var
          else
            val = resolved.expand(var.value.to_s)
            result.add ResolvedVariable.new(var.name, val)
          end
        end
      }
    end
  end

  class ResolvedVariables
    def ResolvedVariables.define
      new.tap {|vars|
        yield vars
        vars.freeze
      }
    end

    def initialize
      @vars = {}
    end

    def inspect
      "\#<#{self.class} #{@vars.inspect}>"
    end

    def resolved?
      true
    end

    def freeze
      @vars.freeze
      super
    end

    def add(var)
      raise "[BUG] unresolved variable: #{var.name}" unless var.resolved?
      @vars[var.name] = var
    end

    def resolve_update(unresolved)
      raise "[BUG] already resolved variables given" if unresolved.resolved?
      unresolved.resolve_with(self).each_variable do |var|
        add var
      end
    end

    def bound?(name)
      @vars.key?(name.to_s)
    end

    def [](name)
      var = @vars[name.to_s] or raise ParameterError, "undefined parameter: #{name}"
      var.value
    end

    def keys
      @vars.keys
    end

    def each_variable(&block)
      @vars.each_value(&block)
    end

    def expand(str)
      Variable.expand_string(str) {|name| self[name] }
    end

    def bind_declarations(decls)
      decls.each do |decl|
        unless bound?(decl.name)
          raise ParameterError, "script parameter not given: #{decl.name}"
        end
      end
    end
  end

  class Variable
    # generic variable extractor
    def Variable.expand_string(str)
      str.gsub(/\$(\w+)|\$\{(\w+)\}/) { yield ($1 || $2) }
    end

    def Variable.list(str)
      str.scan(/\$(\w+)|\$\{(\w+)\}/).flatten.compact.uniq
    end

    def initialize(name, value)
      @name = name
      @value = value
    end

    attr_reader :name
    attr_reader :value

    def resolved?
      false
    end

    def inspect
      "\#<#{self.class} #{@name}=#{@value.inspect}>"
    end
  end

  class ResolvedVariable < Variable
    def resolved?
      true
    end
  end

end
