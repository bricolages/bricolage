require 'json'

module Bricolage

  class SQSClientWrapper
    def initialize(sqs, logger:)
      @sqs = sqs
      @logger = logger
    end

    def receive_message(**args)
      @logger.debug "receive_message(#{args.inspect})"
      @sqs.receive_message(**args)
    end

    def send_message(**args)
      @logger.debug "send_message(#{args.inspect})"
      @sqs.send_message(**args)
    end

    def delete_message(**args)
      @logger.debug "delete_message(#{args.inspect})"
      @sqs.delete_message(**args)
    end
  end


  class DummySQSClient
    def initialize(queue = [])
      @queue = queue
    end

    def receive_message(**args)
      msg_recs = @queue.shift or return EMPTY_RESULT
      msgs = msg_recs.map {|recs| Message.new({'Records' => recs}.to_json) }
      Result.new(true, msgs)
    end

    def send_message(**args)
      SUCCESS_RESULT
    end

    def delete_message(**args)
      SUCCESS_RESULT
    end

    class Result
      def initialize(successful, messages = nil)
        @successful = successful
        @messages = messages
      end

      def successful?
        @successful
      end

      attr_reader :messages
    end

    SUCCESS_RESULT = Result.new(true)
    EMPTY_RESULT = Result.new(true, [])

    class Message
      def initialize(body)
        @body = body
      end

      attr_reader :body
    end
  end

end   # module Bricolage
