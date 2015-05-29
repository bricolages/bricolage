JobClass.define('wait-file') {
  parameters {|params|
    params.add DestFileParam.new
    params.add StringParam.new('condition-expr', 'RUBY_EXPR', 'Wait condition expression (in Ruby).  (default: check file existance)', optional: true)
    params.add StringParam.new('max-wait-minutes', 'NUM', 'Max waiting minutes.')
  }

  script {|params, script|
    script.task(params.generic_ds) {|task|
      dest_file = params['dest-file']
      max_wait_minutes = params['max-wait-minutes'].to_i

      default_expr = "File.exist?('#{dest_file}')"
      condition_expr = params['condition-expr'] || default_expr

      task.action("wait #{dest_file} (condition: #{condition_expr})") {|ds|
        wait_file_condition(dest_file, condition_expr, max_wait_minutes, ds.logger)
      }
    }
  }

  REPORT_INTERVAL = 90   # 15 min
  WAIT_INTERVAL = 5

  def wait_file_condition(dest_file, condition_expr, max_wait_minutes, logger)
    logger.info "waiting file by { #{condition_expr} }"

    # DO NOT CHANGE these variables name; they are condition expression interface.
    start_time = Time.now
    wait_limit = start_time + max_wait_minutes * 60
    last_log_time = start_time

    until eval(condition_expr)
      now = Time.now
      if now > wait_limit
        logger.error "exceeded wait limit (#{max_wait_minutes} minutes); abort"
        return JobResult.for_bool(false)
      end
      if now - last_log_time > REPORT_INTERVAL
        logger.info "waiting..."
        last_log_time = Time.now
      end
      sleep WAIT_INTERVAL
    end
    logger.info "condition fullfilled"
    JobResult.for_bool(true)
  end
}
