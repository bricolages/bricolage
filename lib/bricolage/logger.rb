require 'stringio'
require 'logger'

module Bricolage
  class Logger < ::Logger
    def Logger.default
      @default ||= new
    end

    DEFAULT_ROTATION_SIZE = 1024 ** 2 * 100   # 100MB

    def Logger.new(device: $stderr, rotation_period: nil, rotation_size: DEFAULT_ROTATION_SIZE)
      logger = super(device, (rotation_period || 0), rotation_size)
      logger.level = (device == $stderr && $stderr.tty?) ? Logger::DEBUG : Logger::INFO
      logger.formatter = -> (sev, time, prog, msg) {
        "#{time}: #{sev}: #{msg}\n"
      }
      logger
    end

    def exception(ex)
      buf = StringIO.new
      buf.puts "#{ex.class}: #{ex.message}"
      ex.backtrace.each do |trace|
        buf.puts "\t" + trace
      end
      error buf.string
    end

    def with_elapsed_time(label = '')
      start_time = Time.now
      begin
        return yield
      ensure
        elapsed_time(label, Time.now - start_time)
      end
    end

    def elapsed_time(label, t)
      info "#{label}#{pretty_interval(t)}"
    end

    private

    def pretty_interval(seconds)
      case
      when seconds > 60 * 60
        h, secs = seconds.divmod(60 * 60)
        m, s = secs.divmod(60)
        "%d hours %d minutes" % [h, m]
      when seconds > 60
        "%d minutes %d seconds" % seconds.divmod(60)
      else
        "%.2f secs" % seconds
      end
    end
  end
end
