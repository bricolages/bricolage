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
      attr_accessor :variable_yml_vars
      alias load_global_variables variable_yml_vars
    end

    test "global variable precedence" do
      opt_gvars = Variables.new
      opt_gvars.add Variable.new('var_global_opt', 'loc_global_opt')
      fs = DummyFS.new('/home/path')
      ctx = DummyContextForGvarTest.new(fs, 'development', global_variables: opt_gvars)
      ctx.variable_yml_vars = Variables.define {|vars|
        vars.add Variable.new('var_variable_yml', 'loc_variable_yml')
        vars.add Variable.new('var_global_opt', 'loc_variable_yml')
      }
      result = ctx.global_variables

      assert_equal 'loc_variable_yml', result['var_variable_yml']
      assert_equal 'loc_global_opt', result['var_global_opt']
    end

    DummyContext = Struct.new(:global_variables)
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
      gvars = Variables.new
      gvars.add Variable.new('ow_global_variable', 'loc_global_variable')
      gvars.add Variable.new('ow_rest_var', 'loc_global_variable')
      gvars.add Variable.new('ow_job_opt', 'loc_global_variable')
      ctx = DummyContext.new(gvars)
      job_class = DummyJobClass.new
      job = Job.new('varprec', job_class, ctx)
      job.init_global_variables
      job.bind_parameters({
        'ow_rest_var' => 'loc_rest_var',
        'ow_job_opt' => 'loc_rest_var'
      })
      job.parsing_options {|h|
        opts = OptionParser.new
        h.define_options(opts)
        opts.parse!(['-v', 'ow_job_opt=loc_job_opt'])
      }
      job.compile

      assert_equal 'loc_global_variable', job.variables['ow_global_variable']
      assert_equal 'loc_rest_var', job.variables['ow_rest_var']
      assert_equal 'loc_job_opt', job.variables['ow_job_opt']
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
