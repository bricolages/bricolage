require 'bricolage/datasource'
require 'bricolage/sqswrapper'
require 'aws-sdk'
require 'json'
require 'time'

module Bricolage

  class SQSDataSource < DataSource

    declare_type 'sqs'

    def initialize(region: 'ap-northeast-1', url:, access_key_id:, secret_access_key:,
        visibility_timeout:, max_number_of_messages: 10, wait_time_seconds: 20, noop: false)
      @region = region
      @url = url
      @access_key_id = access_key_id
      @secret_access_key = secret_access_key
      @visibility_timeout = visibility_timeout
      @max_number_of_messages = max_number_of_messages
      @wait_time_seconds = wait_time_seconds
      @noop = noop
    end

    attr_reader :region
    attr_reader :access_key_id
    attr_reader :secret_access_key

    def client
      @client ||= begin
        c = @noop ? DummySQSClient.new : Aws::SQS::Client.new(region: @region, access_key_id: @access_key_id, secret_access_key: @secret_access_key)
        SQSClientWrapper.new(c, logger: logger)
      end
    end

    #
    # High-Level Polling Interface
    #

    def main_handler_loop(handlers)
      trap_signals

      n_zero = 0
      until terminating?
        insert_handler_wait(n_zero)
        n_msg = handle_messages(handlers)
        if n_msg == 0
          n_zero += 1
        else
          n_zero = 0
        end
      end
      logger.info "shutdown gracefully"
    end

    def trap_signals
      # Allows graceful stop
      Signal.trap(:TERM) {
        initiate_terminate
      }
    end

    def initiate_terminate
      # No I/O allowed in this method
      @terminating = true
    end

    def terminating?
      @terminating
    end

    def insert_handler_wait(n_zero)
      sec = 2 ** [n_zero, 6].min   # max 64s
      logger.info "queue wait: sleep #{sec}" if n_zero > 0
      sleep sec
    end

    def handle_messages(handlers:, message_class:)
      n_msg = foreach_message(message_class) do |msg|
        logger.debug "handling message: #{msg.inspect}" if logger.debug?
        mid = "handle_#{msg.message_type}"
        # just ignore unknown event to make app migration easy
        if handlers.respond_to?(mid, true)
          handlers.__send__(mid, msg)
        else
          logger.error "unknown SQS message type: #{msg.message_type.inspect} (message-id: #{msg.message_id})"
        end
      end
      n_msg
    end

    def foreach_message(message_class, &block)
      result = receive_messages()
      unless result and result.successful?
        logger.error "ReceiveMessage failed: #{result ? result.error.message : '(result=nil)'}"
        return nil
      end
      logger.info "receive #{result.messages.size} messages" unless result.messages.empty?
      msgs = message_class.for_sqs_result(result)
      msgs.each(&block)
      msgs.size
    end

    #
    # API-Level Interface
    #

    def receive_messages
      result = client.receive_message(
        queue_url: @url,
        attribute_names: ["All"],
        message_attribute_names: ["All"],
        max_number_of_messages: @max_number_of_messages,
        visibility_timeout: @visibility_timeout,
        wait_time_seconds: @wait_time_seconds
      )
      result
    end

    def delete_message(msg)
      # TODO: use batch request?
      client.delete_message(
        queue_url: @url,
        receipt_handle: msg.receipt_handle
      )
    end

    def put(msg)
      send_message(msg)
    end

    def send_message(msg)
      client.send_message(
        queue_url: @url,
        message_body: { 'Records' => [msg.body] }.to_json,
        delay_seconds: msg.delay_seconds
      )
    end

  end   # class SQSDataSource


  class SQSMessage

    SQS_EVENT_SOURCE = 'bricolage:system'

    # Writer interface
    def SQSMessage.create(
        name:,
        time: Time.now.getutc,
        source: SQS_EVENT_SOURCE,
        delay_seconds: 0,
        **message_params)
      new(name: name, time: time, source: source, delay_seconds: delay_seconds, **message_params)
    end

    def SQSMessage.for_sqs_result(result)
      result.messages.flat_map {|msg|
        body = JSON.parse(msg.body)
        records = body['Records'] or next []
        records.map {|rec| get_concrete_class(msg, rec).for_sqs_record(msg, rec) }
      }
    end

    # abstract SQSMessage.get_concrete_class(msg, rec)

    def SQSMessage.for_sqs_record(msg, rec)
      new(** SQSMessage.parse_sqs_record(msg, rec).merge(parse_sqs_record(msg, rec)))
    end

    def SQSMessage.parse_sqs_record(msg, rec)
      time_str = rec['eventTime']
      tm = time_str ? (Time.parse(time_str) rescue nil) : nil
      {
        message_id: msg.message_id,
        receipt_handle: msg.receipt_handle,
        name: rec['eventName'],
        time: tm,
        source: rec['eventSource']
      }
    end

    def initialize(name:, time:, source:,
        message_id: nil, receipt_handle: nil, delay_seconds: nil,
        **message_params)
      @name = name
      @time = time
      @source = source

      @message_id = message_id
      @receipt_handle = receipt_handle

      @delay_seconds = delay_seconds

      init_message(**message_params)
    end

    # abstract init_message(**message_params)

    attr_reader :name
    attr_reader :time
    attr_reader :source

    # Valid only for received messages

    attr_reader :message_id
    attr_reader :receipt_handle

    # Valid only for sending messages

    attr_reader :delay_seconds

    def body
      obj = {}
      [
        ['eventName', @name],
        ['eventTime', (@time ? @time.iso8601 : nil)],
        ['eventSource', @source]
      ].each do |name, value|
        obj[name] = value if value
      end
      obj
    end

  end   # class SQSMessage

end   # module Bricolage
