require 'fileutils'
require 'tmpdir'

module Bricolage

  module CommandUtils
    def command(*args, env: nil)
      logger.info "command: #{args.join(' ')}"
      sargs = args.map {|a| a.to_s }
      sargs.unshift env if env
      system(*sargs)
      st = $?
      logger.info "status: #{st.exitstatus} (#{st})"
      st
    end

    def make_tmpfile(content, tmpdir: Dir.tmpdir)
      path = new_tmpfile_path(tmpdir)
      File.open(path, 'w') {|f|
        f.write content
      }
      yield path
    ensure
      FileUtils.rm_f path
    end

    def new_tmpfile_path(tmpdir = Dir.tmpdir)
      "#{tmpdir}/#{Time.now.to_i}_#{$$}_#{'%x' % Thread.current.object_id}_#{rand(2**16)}"
    end

    # CLUDGE: FIXME: bricolage-jobnet command writes stderr to the file, we can find error messages from there.
    # Using a temporary file or Ruby SQL driver is **MUCH** better.
    def retrieve_last_match_from_stderr(re, nth = 0)
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
  end

end
