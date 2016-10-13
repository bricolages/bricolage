require 'bricolage/datasource'
require 'aws-sdk'

module Bricolage

  class SNSDataSource < DataSource

    declare_type 'sns'

    def initialize(region:, topic_arn:, access_key_id: nil, secret_access_key: nil)
      @region = region
      @topic_arn = topic_arn
      @access_key_id = access_key_id
      @secret_access_key = secret_access_key
      @client = Aws::SNS::Client.new(region: region, access_key_id: access_key_id, secret_access_key: secret_access_key)
      @topic = Aws::SNS::Topic.new(topic_arn, client: @client)
    end

    attr_reader :region
    attr_reader :client
    attr_reader :topic

    def publish(message)
      @topic.publish({ message: message })
    rescue Aws::SNS::Errors::InvalidParameter => ex
      raise JobError, "bad SNS configuration (topic_arn=#{@topic_arn.inspect}): #{ex.message}"
    rescue Aws::SNS::Errors::ServiceError => ex
      raise SNSException.wrap(ex)
    end

    # IO compatible methods as a logger device

    alias write publish

    def close
    end

  end   # class SNSDataSource

end   # module Bricolage
