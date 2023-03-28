require 'bricolage/sqlstatement'
require 'bricolage/resource'
require 'bricolage/embeddedcodeapi'
require 'bricolage/exception'
require 'pathname'
require 'yaml'
require 'erb'
require 'date'

module Bricolage

  class ConfigLoader
    def initialize(app_home)
      @app_home = app_home
      @base_dir = Pathname('.')
    end

    def load_yaml(path)
      parse_yaml(load_eruby(path), path)
    end

    def load_eruby(path)
      eruby(load_file(path), path)
    end

    alias load_text load_eruby   # FIXME: obsolete

    def load_file(path)
      File.read(path)
    rescue SystemCallError => err
      raise ParameterError, "could not read file: #{err.message}"
    end

    def eruby(text, path)
      erb = ERB.new(text, trim_mode: '%-')
      erb.filename = path.to_s
      push_base_dir(path) {
        erb.result(binding())
      }
    end

    private

    def parse_yaml(text, path)
      if Gem::Version.new(YAML::VERSION) >= Gem::Version.new("4.0.0")
        YAML.load(text, aliases: true)
      else
        YAML.load(text)
      end
    rescue => err
      raise ParameterError, "#{path}: config file syntax error: #{err.message}"
    end

    #
    # For embedded code
    #

    include EmbeddedCodeAPI

    def app_home
      @app_home or raise ParameterError, "app_home is not given in this file"
    end

    def base_dir
      @base_dir
    end

    def push_base_dir(path)
      saved, @base_dir = @base_dir, Pathname(path).parent
      begin
        yield
      ensure
        @base_dir = saved
      end
    end

    # $base_dir + "vars.yml" -> "$base_dir/vars.yml"
    # $base_dir + "/abs/path/vars.yml" -> "/abs/path/vars.yml"
    def read_config_file(path)
      load_eruby(relative_path(Pathname(path)))
    end
  end

end
