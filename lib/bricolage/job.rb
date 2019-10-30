require 'bricolage/jobfile'
require 'bricolage/jobclass'
require 'bricolage/jobresult'
require 'bricolage/parameters'
require 'bricolage/variables'
require 'bricolage/configloader'
require 'bricolage/loglocator'
require 'bricolage/exception'
require 'fileutils'

module Bricolage

  class Job
    # For JobNetRunner
    def Job.load_ref(ref, jobnet_context)
      ctx = jobnet_context.subsystem(ref.subsystem)
      path = ctx.job_file(ref.name)
      load_file(path, ctx)
    end

    # For standalone job (.job file mode)
    def Job.load_file(path, ctx)
      f = JobFile.load(ctx, path)
      instantiate(f.job_id, f.class_id, ctx).tap {|job|
        job.bind_parameters f.values
        f.global_variables.each do |name, value|
          job.global_variables[name.to_s] = value
        end
      }
    end

    # For standalone job (command line mode)
    def Job.instantiate(id, class_id, ctx)
      new(id, JobClass.get(class_id), ctx).tap {|job|
        job.init_global_variables
      }
    end

    def initialize(id, job_class, context)
      @id = id
      @job_class = job_class
      @context = context
      @global_variables = nil
      @option_variables = @context.option_variables
      @param_decls = @job_class.get_parameters
      @param_vals = nil      # Parameters::IntermediateValues by *.job
      @param_vals_opt = nil  # Parameters::IntermediateValues by options
      @params = nil
      @variables = nil
    end

    attr_reader :id

    def class_id
      @job_class.id
    end

    def subsystem
      @context.subsystem_name
    end

    def init_global_variables
      # Context#global_variables loads file on each call,
      # updating @global_variables is multi-thread safe.
      @global_variables = @context.global_variables
      @global_variables['bricolage_cwd'] = Dir.pwd
      @global_variables['bricolage_job_dir'] = @context.job_dir.to_s
    end

    attr_reader :params
    attr_reader :global_variables   # valid after #init_global_variables
    attr_reader :variables          # valid after #compile
    attr_reader :script             # valid after #compile

    # For job file
    def bind_parameters(values)
      @param_vals = @param_decls.parse_direct_values(values)
    end

    # For command line options
    def parsing_options(&block)
      @param_vals_opt = @param_decls.parsing_options(&block)
    end

    def compile
      param_vals_default = @param_decls.parse_default_values(@global_variables.get_force('defaults'))
      @job_class.invoke_parameters_filter(self)

      job_file_rest_vars = @param_vals ? @param_vals.variables : Variables.new
      job_v_opt_vars = @param_vals_opt ? @param_vals_opt.variables : Variables.new
      cmd_v_opt_vars = @option_variables ? @option_variables : Variables.new

      # We use different variable set for paramter expansion and
      # SQL variable expansion.  Parameter expansion uses global
      # variables and "-v" option variables (both of global and job).
      base_vars = Variables.union(
        #          ^ Low precedence
        @global_variables,
        cmd_v_opt_vars,
        job_v_opt_vars
        #          v High precedence
      )
      pvals = @param_decls.union_intermediate_values(*[param_vals_default, @param_vals, @param_vals_opt].compact)
      @params = pvals.resolve(@context, base_vars.resolve)

      # Then, expand SQL variables and check with declarations.
      vars = Variables.union(
        #          ^ Low precedence
        declarations.default_variables, # defined by jobclass
        @global_variables,   # from yaml file
        @params.variables,   # Like $dest_table in job file
        job_file_rest_vars,  # custom variable at header of job file
        cmd_v_opt_vars,      # -v option for jobnet command
        job_v_opt_vars       # -v option for job command
        #          v High precedence
      )
      @variables = vars.resolve
      @variables.bind_declarations declarations

      @script = @job_class.get_script(@params)
      @script.bind @context, @variables
    end

    def provide_default(name, value)
      @param_vals[name] ||= value if @param_vals
    end

    # Called from jobclasses (parameters_filter)
    def provide_sql_file_by_job_id
      provide_default 'sql-file', @id if @id
    end

    def declarations
      @declarations ||= @job_class.get_declarations(@params)
    end

    def script_source
      raise 'Job#script_source called before #compile' unless @script
      @script.source
    end

    def explain
      raise 'Job#explain called before #compile' unless @script
      @script.run_explain
    end

    def execute(log_locator: LogLocator.empty)
      log_locator.redirect_stdouts {
        do_execute
      }
    end

    def execute_in_process(log_locator:)
      # ??? FIXME: status_path should be independent from log_path.
      # Also, status_path should be defined regardless of log_path.
      status_path = log_locator.path ? "#{log_locator.path}.status" : nil
      isolate_process(status_path) {
        log_locator.redirect_stdouts {
          do_execute
        }
      }
    end

    private

    def do_execute
      ENV['BRICOLAGE_PID'] = Process.pid.to_s
      logger = @context.logger
      logger.info "#{@context.environment} environment"
      result = logger.with_elapsed_time {
        script.run
      }
      logger.info result.status_string
      result
    rescue JobFailure => ex
      logger.error ex.message
      logger.error "failure: #{ex.message}"
      return JobResult.failure(ex)
    rescue Exception => ex
      logger.exception ex
      logger.error "error: #{ex.class}: #{ex.message}"
      return JobResult.error(ex)
    end

    def isolate_process(status_path)
      cpid = Process.fork {
        Process.setproctitle "bricolage [#{@id}]"
        result = yield
        save_result result, status_path
        exit result.status
      }
      _, st = Process.waitpid2(cpid)
      restore_result(st, status_path)
    end

    def save_result(result, status_path)
      return if result.success?
      return unless status_path
      begin
        File.open(status_path, 'w') {|f|
          f.puts result.message
        }
      rescue
      end
    end

    def restore_result(st, status_path)
      JobResult.for_process_status(st, restore_message(status_path))
    end

    def restore_message(status_path)
      return nil unless status_path
      begin
        msg = read_if_exist(status_path)
        msg ? msg.strip : nil
      ensure
        FileUtils.rm_f status_path
      end
    end

    def read_if_exist(path)
      File.read(path)
    rescue
      nil
    end
  end

end
