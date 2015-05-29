JobClass.define('exec') {
  parameters {|params|
    params.add StringListParam.new('args', 'ARG', 'Command line arguments.')
  }

  script {|params, script|
    script.task(params.generic_ds) {|task|
      task.action(params['args'].join(' ')) {|ds|
        ds.logger.info '[CMD] ' + params['args'].join(' ')
        system(*params['args'])
        st = $?
        ds.logger.info "status=#{st.exitstatus}"
        JobResult.for_bool(st.exitstatus == 0)
      }
    }
  }
}
