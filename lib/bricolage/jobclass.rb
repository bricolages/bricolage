require 'bricolage/parameters'
require 'bricolage/script'
require 'bricolage/sqlstatement'
require 'bricolage/exception'
require 'pathname'

module Bricolage

  class JobClass
    CLASSES = {}

    def JobClass.define(id, &block)
      id = id.to_s
      raise FatalError, "duplicated job class: #{@id.inspect}" if CLASSES[id]
      c = new(id)
      c.instance_exec(c, &block)
      CLASSES[id] = c
    end

    srcdir = Pathname(__FILE__).realpath.parent.parent.parent.cleanpath
    LOAD_PATH = [srcdir + 'jobclass']

    def JobClass.get(id)
      unless CLASSES[id.to_s]
        begin
          path = LOAD_PATH.map {|prefix| prefix + "#{id}.rb" }.detect(&:exist?)
          raise ParameterError, "no such job class: #{id}" unless path
          ::Bricolage.module_eval File.read(path), path.to_path, 1
        rescue SystemCallError => err
          raise FatalError, "could not load job class: #{id}: #{err.message}"
        end
        raise FatalError, "job class file loaded but required job class is not defined: #{id}" unless CLASSES[id.to_s]
      end
      CLASSES[id.to_s]
    end

    def JobClass.list
      LOAD_PATH.map {|dir|
        Dir.glob("#{dir}/*.rb").map {|path| File.basename(path, '.rb') }
      }.flatten.uniq.sort
    end

    def JobClass.each(&block)
      CLASSES.each_value(&block)
    end

    def initialize(id)
      @id = id
      @parameters = nil
      @parameters_filter = nil   # optional
      @declarations = nil
      @script = nil
    end

    attr_reader :id

    def inspect
      "\#<#{self.class} #{@id}>"
    end

    def parameters(&block)
      @parameters = block
    end

    def get_parameters
      Parameters::Declarations.new.tap {|params|
        @parameters.call(params)
      }
    end

    def parameters_filter(&block)
      @parameters_filter = block
    end

    def invoke_parameters_filter(job)
      @parameters_filter.call(job) if @parameters_filter
    end

    def declarations(&block)
      @declarations = block
    end

    def get_declarations(params)
      @declarations ? @declarations.call(params) : Declarations.new
    end

    def script(&block)
      @script = block
    end

    def get_script(params)
      Script.new.tap {|script|
        @script.call(params, script)
      }
    end
  end

end
