require 'bricolage/application'
require 'bricolage/context'
require 'forwardable'

module Bricolage
  class CommandLineApplication
    def CommandLineApplication.define
      Application.install_signal_handlers
      opts = new
      yield opts
      opts.parse!
      opts
    end

    def initialize
      @name = File.basename($0)
      @home = nil
      @env = nil
      @subsys = nil
      @ds = {}
      @options = OptionParser.new
      @options.banner = "Usage: #{@name} [options]"
      define_default_options
    end

    attr_reader :name
    attr_reader :options

    def define_default_options
      @options.on_tail('-e', '--environment=ENV', "Bricolage execution environment. (default: #{Context.environment})") {|env|
        @env = env
      }
      default_home = Context.home_path
      @options.on_tail('-C', '--home=PATH', "Bricolage home directory. (default: #{default_home == Dir.getwd ? '.' : default_home})") {|path|
        @home = path
      }
      @options.on_tail('--help', 'Prints this message and quit.') {
        puts @options.help
        exit 0
      }
    end

    extend Forwardable
    def_delegator '@options', :on
    def_delegator '@options', :help

    DataSourceOpt = Struct.new(:kind, :name, :ds)

    def data_source_option(long_option, description, kind:, short: nil, default: nil)
      @ds[long_option] = DataSourceOpt.new(kind, default || kind)
      @options.on(* [short, long_option + "=NAME", description + " (default: #{default || kind})"].compact) {|ds_name|
        @ds[long_option] = DataSourceOpt.new(kind, ds_name)
      }
    end

    def data_source(long_option)
      ent = @ds[long_option] or raise ArgumentError, "no such data source entry: #{long_option}"
      ent.ds ||= context.get_data_source(ent.kind, ent.name)
    end

    def parse!
      return @options.parse!
    rescue OptionParser::ParseError => err
      $stderr.puts "#{$0}: error: #{err.message}"
      $stderr.puts @options.help
      exit 1
    end

    def context
      @context ||= create_context
    end

    def create_context
      Bricolage::Context.for_application(@home, environment: @env)
    end

    def main
      yield
    rescue => ex
      $stderr.puts "#{@name}: error: #{ex.message}"
      exit 1
    end
  end
end
