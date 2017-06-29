module Bricolage
  class LogLocator
    def LogLocator.empty
      new(nil, nil)
    end

    def initialize(path, s3_writer)
      @path = path
      @s3_writer = s3_writer
    end

    attr_reader :path

    def s3_url
      return nil unless @s3_writer
      @s3_writer.url
    end

    def redirect_stdouts
      return yield unless @path
      FileUtils.mkdir_p File.dirname(@path)
      @original_stdout = $stdout.dup
      @original_stderr = $stderr.dup
      begin
        # Use 'w+' to make readable for retrieve_last_match_from_stderr
        File.open(@path, 'w+') {|f|
          f.sync = true
          $stdout.reopen f
          $stderr.reopen f
        }
        return yield
      ensure
        $stdout.reopen @original_stdout; @original_stdout.close
        $stderr.reopen @original_stderr; @original_stderr.close
        upload
      end
    end

    # CLUDGE: FIXME: We redirect stderr to the file, we can find error messages from there.
    # Using a temporary file or Ruby SQL driver is **MUCH** better.
    def self.slice_last_stderr(re, nth = 0)
      return unless $stderr.stat.file?
      $stderr.flush
      f = $stderr.dup
      matched = nil
      begin
        f.seek(0)
        f.each do |line|
          m = line.slice(re, nth)
          matched = m if m
        end
      ensure
        f.close
      end
      matched = matched.to_s.strip
      matched.empty? ? nil : matched
    end

    def upload
      return unless @path
      return unless @s3_writer
      # FIXME: Shows HTTP URL?
      puts "bricolage: S3 log: #{s3_url}"
      begin
        @s3_writer.upload(path)
      rescue => ex
        puts "warning: S3 upload failed: #{s3_url}"
      end
    end
  end
end
