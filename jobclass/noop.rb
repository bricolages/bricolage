JobClass.define('noop') {
  parameters {|params|
    params.add OptionalBoolParam.new('failure', 'Finish with failure or not.')
  }

  script {|params, script|
    script.task(params.generic_ds) {|task|
      task.action('return !$failure') {
        JobResult.for_bool(!params['failure'])
      }
    }
  }
}
