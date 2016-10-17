require 'bricolage/context'
require 'bricolage/job'
require 'bricolage/jobclass'
require 'bricolage/jobresult'
require 'bricolage/variables'
require 'bricolage/datasource'
require 'bricolage/eventhandlers'
require 'bricolage/postgresconnection'
require 'bricolage/logger'
require 'bricolage/exception'
require 'bricolage/version'
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
    end

    def main
      opts = GlobalOptions.new(self)
      @hooks.run_before_option_parsing_hooks(opts)
      opts.parse ARGV
      @ctx = Context.for_application(opts.home, opts.job_file, environment: opts.environment, global_variables: opts.global_variables)
      if opts.list_global_variables?
        list_variables @ctx.global_variables.resolve
        exit 0
      end
      job = load_job(@ctx, opts)
      process_job_options job, opts
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

      @hooks.run_before_all_jobs_hooks(BeforeAllJobsEvent.new(job.id, [job]))
      @hooks.run_before_job_hooks(BeforeJobEvent.new(job))
      result = job.execute
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

  class GlobalOptions
    def initialize(app)
      @app = app
      @job_file = nil
      @environment = nil
      @home = nil
      @global_variables = Variables.new
      @dry_run = false
      @explain = false
      @list_global_variables = false
      @list_variables = false
      @list_declarations = false
      @parser = OptionParser.new
      define_options @parser
    end

    attr_reader :parser

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
      parser.on('-v', '--variable=NAME=VALUE', 'Set global variable (is different from job-level -v !!).') {|name_value|
        name, value = name_value.split('=', 2)
        @global_variables[name] = value
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

    def parse(argv)
      @parser.order! argv
      @rest_args = argv.dup
    rescue OptionParser::ParseError => ex
      raise OptionError, ex.message
    end

    attr_reader :environment
    attr_reader :home
    attr_reader :global_variables

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

    def list_global_variables?
      @list_global_variables
    end

    def list_variables?
      @list_variables
    end

    def list_declarations?
      @list_declarations
    end
  end

end
