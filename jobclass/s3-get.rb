JobClass.define('s3-get') {
  parameters {|params|
    params.add DestFileParam.new
    params.add SrcFileParam.new
    params.add DataSourceParam.new('s3')
  }

  script {|params, script|
    script.task(params['data-source']) {|task|
      task.get params['src-file'], params['dest-file']
    }
  }
}
