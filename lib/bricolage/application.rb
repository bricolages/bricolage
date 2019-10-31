require 'bricolage/context'
require 'bricolage/job'
require 'bricolage/jobclass'
require 'bricolage/jobresult'
require 'bricolage/jobnet'
require 'bricolage/variables'
require 'bricolage/datasource'
require 'bricolage/eventhandlers'
require 'bricolage/postgresconnection'
require 'bricolage/logfilepath'
require 'bricolage/loglocatorbuilder'
require 'bricolage/logger'
require 'bricolage/exception'
require 'bricolage/version'
require 'fileutils'
require 'pathname'
require 'optparse'

module Bricolage

  class Application
    def Application.install_signal_handlers
      Signal.trap('PIPE', 'IGNORE')
      PostgresConnection.install_signal_handlers
    end

    def Application.main
      install_signal_handlers
      new.main
    end

    def initialize
      @hooks = Bricolage
      @start_time = Time.now
    end

    def main
      opts = GlobalOptions.new(self)
      @hooks.run_before_option_parsing_hooks(opts)
      opts.parse!(ARGV)

      @ctx = Context.for_application(opts.home, opts.job_file, environment: opts.environment, option_variables: opts.option_variables)
      opts.merge_saved_options(@ctx.load_system_options)

      if opts.dump_options?
        opts.option_pairs.each do |name, value|
          puts "#{name}=#{value.inspect}"
        end
        exit 0
      end
      if opts.list_global_variables?
        list_variables @ctx.global_variables.resolve
        exit 0
      end

      job = load_job(@ctx, opts)
      process_job_options(job, opts)
      job.compile

      if opts.list_declarations?
        list_declarations job.declarations
        exit 0
      end
      if opts.list_variables?
        list_variables job.variables
        exit 0
      end
      if opts.dry_run?
        puts job.script_source
        exit 0
      end
      if opts.explain?
        job.explain
        exit 0
      end

      @log_locator_builder = LogLocatorBuilder.for_options(@ctx, opts.log_path_format, opts.log_s3_ds, opts.log_s3_key_format)

      @hooks.run_before_all_jobs_hooks(BeforeAllJobsEvent.new(job.id, [job]))
      @hooks.run_before_job_hooks(BeforeJobEvent.new(job))
      result = job.execute(log_locator: build_log_locator(job))
      @hooks.run_after_job_hooks(AfterJobEvent.new(result))
      @hooks.run_after_all_jobs_hooks(AfterAllJobsEvent.new(result.success?, [job]))
      exit result.status
    rescue OptionError => ex
      raise if $DEBUG
      usage_exit ex.message, opts.help
    rescue ApplicationError => ex
      raise if $DEBUG
      error_exit ex.message
    end

    def build_log_locator(job)
      @log_locator_builder.build(
        job_ref: JobNet::JobRef.new(job.subsystem, job.id, '-'),
        jobnet_id: "#{job.subsystem}/#{job.id}",
        job_start_time: @start_time,
        jobnet_start_time: @start_time
      )
    end

    def load_job(ctx, opts)
      if opts.file_mode?
        Job.load_file(opts.job_file, ctx)
      else
        usage_exit "no job class given", opts.help if ARGV.empty?
        job_class_id = ARGV.shift
        Job.instantiate(nil, job_class_id, ctx)
      end
    rescue ParameterError => ex
      raise if $DEBUG
      usage_exit ex.message, opts.help
    end

    def process_job_options(job, opts)
      parser = OptionParser.new
      parser.banner = "Usage: #{program_name} #{job.class_id} [job_class_options]"
      job.parsing_options {|job_opt_defs|
        job_opt_defs.define_options parser
        parser.on_tail('--help', 'Shows this message and quit.') {
          puts parser.help
          exit 0
        }
        parser.on_tail('--version', 'Shows program version and quit.') {
          puts "#{APPLICATION_NAME} version #{VERSION}"
          exit 0
        }
        parser.parse!
      }
      unless ARGV.empty?
        msg = opts.file_mode? ? "--job-file and job class argument is exclusive" : "bad argument: #{ARGV.first}"
        usage_exit msg, parser.help
      end
    rescue OptionError => ex
      raise if $DEBUG
      usage_exit ex.message, parser.help
    end

    def list_variables(vars)
      vars.each_variable do |var|
        puts "#{var.name}=#{var.value.inspect}"
      end
    end

    def list_declarations(decls)
      decls.each do |decl|
        if decl.have_default_value?
          puts "#{decl.name}\t= #{decl.default_value.inspect}"
        else
          puts decl.name
        end
      end
    end
    
    def usage_exit(msg, usage)
      print_error msg
      $stderr.puts usage
      exit 1
    end

    def error_exit(msg)
      print_error msg
      exit 1
    end

    def print_error(msg)
      $stderr.puts "#{program_name}: error: #{msg}"
    end

    def program_name
      File.basename($PROGRAM_NAME, '.*')
    end
  end

  class CommonApplicationOptions
    OptionValue = Struct.new(:location, :value)
    class OptionValue
      def inspect
        if location
          "#{value.inspect} (#{location})"
        else
          value.inspect
        end
      end
    end

    def init_options
      @opts = nil   # valid only after #parse!
      @opts_default = opts_default()
      @opts_saved = {}
      @opts_env = opts_env()
      @opts_cmdline = {}
      @parser = OptionParser.new
      define_options @parser
    end
    private :init_options

    attr_reader :parser

    def opts_default
      {
        'log-path' => OptionValue.new('default value', nil),
        'log-dir' => OptionValue.new('default value', nil),
        's3-log' => OptionValue.new('default value', nil)
      }
    end
    private :opts_default

    def opts_env
      env = {}
      if path = ENV['BRICOLAGE_LOG_PATH']
        env['log-path'] = OptionValue.new('env BRICOLAGE_LOG_PATH', path)
      end
      if path = ENV['BRICOLAGE_LOG_DIR']
        env['log-dir'] = OptionValue.new('env BRICOLAGE_LOG_DIR', path)
      end
      env
    end
    private :opts_env

    # abstract :define_options

    def define_common_options
      @parser.on('-L', '--log-dir=PATH', 'Log file prefix.') {|path|
        @opts_cmdline['log-dir'] = OptionValue.new('--log-dir option', path)
      }
      @parser.on('--log-path=PATH', 'Log file path template.') {|path|
        @opts_cmdline['log-path'] = OptionValue.new('--log-path option', path)
      }
      @parser.on('--s3-log=DS_KEY', 'S3 log file. (format: "DS:KEY")') {|spec|
        @opts_cmdline['s3-log'] = OptionValue.new('--s3-log option', spec)
      }
    end
    private :define_common_options

    def merge_saved_options(vars)
      saved = {}
      @opts_default.keys.each do |key|
        if val = vars.get_force(key)
          saved[key] = OptionValue.new("bricolage.yml:#{key}", val)
        end
      end
      @opts_saved = saved
      build_common_options!
    end

    def build_common_options!
      @opts = [@opts_default, @opts_saved, @opts_env, @opts_cmdline].inject({}) {|h, opts| h.update(opts); h }
    end
    private :build_common_options!

    #
    # Accessors
    #

    def common_options
      @opts
    end

    def log_path_format
      if opt = @opts['log-dir'] and opt.value
        LogFilePath.new("#{opt.value}/%{std}.log")
      elsif opt = @opts['log-path'] and opt.value
        LogFilePath.new(opt.value)
      else
        nil
      end
    end

    def log_s3_ds
      s3_log_spec.first
    end

    def log_s3_key_format
      s3_log_spec.last
    end

    def s3_log_spec
      @s3_log_spec ||=
        if opt = @opts['s3-log'] and spec = opt.value
          ds, k = spec.split(':', 2)
          k = k.to_s.strip
          key = k.empty? ? nil : k
          [ds, LogFilePath.new(key || '%{std}.log')]
        else
          [nil, nil]
        end
    end
  end

  class GlobalOptions < CommonApplicationOptions
    def initialize(app)
      @app = app
      @job_file = nil
      @environment = nil
      @home = nil
      @option_variables = Variables.new
      @dry_run = false
      @explain = false
      @list_global_variables = false
      @list_variables = false
      @list_declarations = false
      @dump_options = false
      init_options
    end

    def help
      @parser.help
    end

    def define_options(parser)
      parser.banner = <<-EndBanner
Synopsis:
  #{@app.program_name} [global_options] JOB_CLASS [job_options]
  #{@app.program_name} [global_options] --job=JOB_FILE -- [job_options]
Global Options:
      EndBanner
      parser.on('-f', '--job=JOB_FILE', 'Give job parameters via job file (YAML).') {|path|
        @job_file = path
      }
      parser.on('-e', '--environment=NAME', "Sets execution environment [default: #{Context::DEFAULT_ENV}]") {|env|
        @environment = env
      }
      parser.on('-C', '--home=PATH', 'Sets application home directory.') {|path|
        @home = Pathname(path)
      }
      parser.on('-n', '--dry-run', 'Shows job script without executing it.') {
        @dry_run = true
      }
      parser.on('-E', '--explain', 'Applies EXPLAIN to the SQL.') {
        @explain = true
      }

      define_common_options

      parser.on('--list-job-class', 'Lists job class name and (internal) class path.') {
        JobClass.list.each do |name|
          puts name
        end
        exit 0
      }
      parser.on('--list-global-variables', 'Lists global variables.') {
        @list_global_variables = true
      }
      parser.on('--list-variables', 'Lists all variables.') {
        @list_variables = true
      }
      parser.on('--list-declarations', 'Lists script variable declarations.') {
        @list_declarations = true
      }
      parser.on('-r', '--require=FEATURE', 'Requires ruby library.') {|feature|
        require feature
      }
      parser.on('-v', '--variable=NAME=VALUE', 'Set option variable.') {|name_value|
        name, value = name_value.split('=', 2)
        @option_variables[name] = value
      }
      parser.on('--dump-options', 'Shows option parsing result and quit.') {
        @dump_options = true
      }
      parser.on('--help', 'Shows this message and quit.') {
        puts parser.help
        exit 0
      }
      parser.on('--version', 'Shows program version and quit.') {
        puts "#{APPLICATION_NAME} version #{VERSION}"
        exit 0
      }
    end

    def on(*args, &block)
      @parser.on(*args, &block)
    end

    def parse!(argv)
      @parser.order! argv
      @rest_args = argv.dup
      build_common_options!
    rescue OptionParser::ParseError => ex
      raise OptionError, ex.message
    end

    attr_reader :environment
    attr_reader :home

    attr_reader :job_file

    def file_mode?
      !!@job_file
    end

    def dry_run?
      @dry_run
    end

    def explain?
      @explain
    end

    def dump_options?
      @dump_options
    end

    attr_reader :option_variables

    def list_global_variables?
      @list_global_variables
    end

    def list_variables?
      @list_variables
    end

    def list_declarations?
      @list_declarations
    end

    def option_pairs
      common_options.merge({
        'environment' => OptionValue.new(nil, @environment),
        'home' => OptionValue.new(nil, @home)
      })
    end
  end

end
