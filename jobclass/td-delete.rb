JobClass.define('td-delete') {
  parameters {|params|
    params.add DestTableParam.new(optional: false)
    params.add DataSourceParam.new('td')
    params.add DateParam.new('from', 'DATE', 'Start date of logs to delete (%Y-%m-%d).')
    params.add DateParam.new('to', 'DATE', 'End date of logs to delete (%Y-%m-%d).')
  }

  declarations {|params|
    Declarations.new("dest_table" => nil)
  }

  script {|params, script|
    script.task(params['data-source']) {|task|
      task.delete params['dest-table'],
        from: params['from'],
        to: params['to']
    }
  }
}
