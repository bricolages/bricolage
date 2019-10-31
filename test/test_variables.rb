require 'test/unit'
require 'bricolage/job'
require 'bricolage/context'
require 'bricolage/variables'
require 'bricolage/parameters'
require 'optparse'
require 'pp'

module Bricolage
  class TestVaribles < Test::Unit::TestCase
    DummyFS = Struct.new(:home_path)
    class DummyContextForGvarTest < Context
      attr_accessor :variable_yml_vars, :builtin_variables
      alias load_global_variables variable_yml_vars

      def job_dir
        '/job/dir'
      end
    end

    test "global variable precedence" do
      fs = DummyFS.new('/home/path')
      ctx = DummyContextForGvarTest.new(fs, 'development')
      ctx.builtin_variables = Variables.define {|vars|
        # global variables from builtin
        vars.add Variable.new('builtin_variable', 'loc_builtin_value')
        vars.add Variable.new('ow_yml_variable', 'loc_builtin_value')
      }
      ctx.variable_yml_vars = Variables.define {|vars|
        # global variables from yaml file
        vars.add Variable.new('ow_yml_variable', 'loc_yml_value')
      }
      result = ctx.global_variables

      assert_equal 'loc_builtin_value', result['builltin_variable']
      assert_equal 'loc_yml_value', result['ow_yml_variable'] # overwritten
    end

    DummyContext = Struct.new(:global_variables, :option_variables)
    class DummyContext
      def job_dir
        '/job/dir'
      end
    end

    class DummyJobClass
      def get_parameters
        Parameters::Declarations.new
      end

      def invoke_parameters_filter(job)
      end

      def get_declarations(params)
        Declarations.new
      end

      def get_script(params)
        DummyScript.new
      end
    end

    class DummyScript
      def bind(ctx, vars)
        vars
      end
    end

    test "variable precedence (*.job)" do
      # global variables
      gvars = Variables.new
      gvars.add Variable.new('global_variable', 'loc_global_value')
      gvars.add Variable.new('ow_rest_variable', 'loc_global_value')
      gvars.add Variable.new('ow_job_option_variable', 'loc_global_value')

      ctx = DummyContext.new(gvars)
      job_class = DummyJobClass.new
      job = Job.new('varprec', job_class, ctx)
      job.init_global_variables
      job.bind_parameters({
        # custom variable at header of job file
        'ow_rest_variable' => 'loc_rest_value',
        'ow_job_option_variable' => 'loc_rest_value'
      })
      job.parsing_options {|h|
        opts = OptionParser.new
        h.define_options(opts)
        # -v option variable for job command
        opts.parse!(['-v', 'ow_job_option_variable=loc_job_option_value'])
      }
      job.compile

      assert_equal 'loc_global_value', job.variables['global_variable']
      assert_equal 'loc_rest_value', job.variables['ow_rest_variable'] # overwritten
      assert_equal 'loc_job_option_value', job.variables['ow_job_option_variable'] # overwritten
    end

    test "variable precedence (*.jobnet)" do
      # -v option variable for jobnet command
      ovars = Variables.new
      ovars.add Variable.new('ow_jobnet_option_variable', 'loc_option_value')

      fs = DummyFS.new('/home/path')
      ctx = DummyContextForGvarTest.new(fs, 'development', option_variables: ovars)
      ctx.builtin_variables = Variables.define {|vars|
        # global variables from builtin
        vars.add Variable.new('ow_global_variable', 'BUILTIN_VALUE')
        vars.add Variable.new('ow_rest_variable', 'BUILTIN_VALUE')
        vars.add Variable.new('ow_jobnet_option_variable', 'BUILTIN_VALUE')
      }
      ctx.variable_yml_vars = Variables.define {|vars|
        # global variables from yaml file
        vars.add Variable.new('ow_global_variable', 'loc_yml_value')
        vars.add Variable.new('ow_rest_variable', 'loc_yml_value')
        vars.add Variable.new('ow_jobnet_option_variable', 'loc_yml_value')
      }

      job_class = DummyJobClass.new
      job = Job.new('varprec', job_class, ctx)
      job.init_global_variables
      job.bind_parameters({
        # custom variable at header of job file
        'ow_rest_variable' => 'loc_rest_value',
        'ow_jobnet_option_variable' => 'loc_rest_value',
      })
      job.compile

      assert_equal 'loc_yml_value', job.variables['ow_global_variable']
      assert_equal 'loc_rest_value', job.variables['ow_rest_variable'] # overwritten
      assert_equal 'loc_option_value', job.variables['ow_jobnet_option_variable'] # overwritten
    end

    test "lazy reference resolution" do
      gvars = Variables.new
      gvars.add Variable.new('gvar', 'GVAR')
      gvars.add Variable.new('rest_var', '*global*')
      gvars.add Variable.new('job_opt', '*global*')
      gvars.add Variable.new('ref_gvar', '$gvar')
      gvars.add Variable.new('ref_rest_var', '$rest_var')
      gvars.add Variable.new('ref_job_opt', '$job_opt')
      ctx = DummyContext.new(gvars)
      job_class = DummyJobClass.new
      job = Job.new('lazyres', job_class, ctx)
      job.init_global_variables
      job.bind_parameters({
        'rest_var' => 'REST_VAR'
      })
      job.parsing_options {|h|
        opts = OptionParser.new
        h.define_options(opts)
        opts.parse!(['-v', 'job_opt=JOB_OPT'])
      }
      job.compile

      assert_equal 'GVAR', job.variables['ref_gvar']
      assert_equal 'REST_VAR', job.variables['ref_rest_var']
      assert_equal 'JOB_OPT', job.variables['ref_job_opt']
    end
  end
end
