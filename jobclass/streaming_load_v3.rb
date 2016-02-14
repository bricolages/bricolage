require 'bricolage/rubyjobclass'
require 'bricolage/psqldatasource'

class StreamingLoadJobClassV3 < RubyJobClass

  job_class_id 'streaming_load_v3'

  def self.parameters(params)
    super
    params.add Bricolage::DataSourceParam.new('sql', 'data-source', 'Target Redshift data source.')
    params.add Bricolage::DestTableParam.new(optional: false)
    params.add Bricolage::DestTableParam.new('work-table', optional: true)
    params.add Bricolage::DestTableParam.new('log-table', optional: true)
    params.add Bricolage::KeyValuePairsParam.new('load-options', 'OPTIONS', 'Loader options.',
        optional: true, default: Bricolage::PSQLLoadOptions.new,
        value_handler: lambda {|value, ctx, vars| Bricolage::PSQLLoadOptions.parse(value) })
    params.add Bricolage::SQLFileParam.new('sql-file', 'PATH', 'SQL to insert rows from the work table to the target table.', optional: true)
    params.add Bricolage::DataSourceParam.new('s3', 's3-ds', 'Control bucket data source.')
    params.add Bricolage::OptionalBoolParam.new('noop', 'Does not change any data.')
  end

  def self.declarations(params)
    Bricolage::Declarations.new(
      'dest_table' => nil,
      'work_table' => nil,
      'log_table' => nil
    )
  end

  def initialize(params)
    ds = params['data-source']
    @loader = StreamingLoad::Loader.new(
      data_source: ds,
      table: string(params['dest-table']),
      work_table: string(params['work-table']),
      log_table: string(params['log-table']),
      load_options: params['load-options'],
      sql: params['sql-file'],
      logger: ds.logger,
      noop: params['noop']
    )
  end

  def string(obj)
    obj ? obj.to_s : nil
  end
  private :string

  def run
    @loader.load
    nil
  end

  def bind(ctx, vars)
    @loader.sql.bind(ctx, vars) if @loader.sql
  end

end
