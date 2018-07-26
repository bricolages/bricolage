require 'bricolage/script'
require 'bricolage/configloader'
require 'bricolage/exception'

module Bricolage

  class DataSourceFileNotExist < ParameterError; end

  class DataSourceFactory
    DATA_SOURCE_FILE_NAME_1 = 'datasource.yml'
    DATA_SOURCE_FILE_NAME_2 = 'database.yml'
    PASSWORD_FILE_NAME = 'password.yml'

    def DataSourceFactory.load(context, logger)
      loader = Loader.new(context, logger)
      loader.load_passwords(PASSWORD_FILE_NAME)
      begin
        return loader.load(DATA_SOURCE_FILE_NAME_1)
      rescue DataSourceFileNotExist
        return loader.load(DATA_SOURCE_FILE_NAME_2)
      end
    end

    class Loader < ConfigLoader
      def initialize(context, logger)
        super context.home_path
        @context = context
        @logger = logger
        @passwords = nil
      end

      def load_passwords(basename)
        @context.config_pathes(basename).each do |path|
          if path.exist?
            @passwords = load_yaml(path)
            break
          end
        end
      end

      def load(basename)
        yml_path = @context.config_pathes(basename).detect(&:exist?)
        raise DataSourceFileNotExist, "database.yml does not exist" unless yml_path
        @config_dir = yml_path.parent
        DataSourceFactory.new(load_yaml(yml_path), @context, @logger)
      end

      def password(name)
        (@passwords || {})[name] or raise ParameterError, "no such password entry: #{name}"
      end
    end

    def initialize(configs, context, logger)
      @configs = configs
      @context = context
      @logger = logger
    end

    BUILTIN_TYPES = %w(generic file ruby)

    # For job classes
    def get(kind, name)
      if BUILTIN_TYPES.include?(kind)
        return DataSource.new_for_type(kind, kind, {}, @context, @logger)
      end
      entry_name = name || kind
      conf = config(entry_name)
      type = conf.delete(:type)
      DataSource.new_for_type(type, entry_name, conf, @context, @logger)
    end

    # Ruby API
    def [](name)
      if BUILTIN_TYPES.include?(name)
        return DataSource.new_for_type(name, name, {}, @context, @logger)
      end
      conf = config(name)
      type = conf.delete(:type)
      DataSource.new_for_type(type, name, conf, @context, @logger)
    end

    private

    def config(key)
      ent = @configs[key] or raise ParameterError, "no such data source entry: #{key}"
      canonicalize(ent)
    end

    def canonicalize(config)
      h = {}
      config.each do |k, v|
        h[k.intern] = v
      end
      h
    end
  end

  class DataSource
    def DataSource.new_for_type(type, name, config, context, logger)
      ds = get_class(type).new(**config)
      ds.__send__ :initialize_base, name, context, logger
      ds
    rescue ArgumentError => err
      # FIXME: do not rely on error message
      ent = err.message.slice(/unknown keyword: (\S+)/, 1) or raise
      raise ParameterError, "unknown config entry in database.yml: #{name}.#{ent}"
    end

    CLASSES = {}

    class << self
      private

      def declare_type(type)
        CLASSES[type.to_s] = self
      end
    end

    def DataSource.get_class(type)
      unless CLASSES[type.to_s]
        begin
          require "bricolage/#{type}datasource"
        rescue LoadError
          raise ParameterError, "no such data source type: #{type}"
        end
        raise FatalError, "DataSource class does not exist: #{type}" unless CLASSES[type.to_s]
      end
      CLASSES[type.to_s]
    end

    def initialize_base(name, context, logger)
      @name = name
      @context = context
      @logger = logger
    end
    private :initialize_base

    attr_reader :name
    attr_reader :context
    attr_reader :logger

    def open
      yield nil
    end

    def open_for_batch(&block)
      open(&block)
    end
  end

end
