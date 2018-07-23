JobClass.define('exec') {
  parameters {|params|
    params.add StringListParam.new('args', 'ARG', 'Command line arguments.', allow_string: true)
    params.add KeyValuePairsParam.new('env', 'ENVS', 'Environment variables.')
  }

  script {|params, script|
    script.task(params.generic_ds) {|task|
      task.action(params['args'].join(' ')) {|ds|
        ds.logger.info '[CMD] ' + params['args'].join(' ')
        environ = params['env'] || {}
        argv = params['args']
        system(environ, *argv)
        st = $?
        ds.logger.info "status=#{st.exitstatus}"
        JobResult.for_bool(st.exitstatus == 0)
      }
    }
  }
}
