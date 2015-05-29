JobClass.define('s3-put') {
  parameters {|params|
    params.add DestFileParam.new
    params.add SrcFileParam.new
    params.add OptionalBoolParam.new('remove', 'Removes source files after PUT is succeeded.')
    params.add DataSourceParam.new('s3')
  }

  script {|params, script|
    script.task(params['data-source']) {|task|
      task.put params['src-file'], params['dest-file']
    }
    if params['remove']
      script.task(params.file_ds) {|task|
        task.remove params['src-file']
      }
    end
  }
}
