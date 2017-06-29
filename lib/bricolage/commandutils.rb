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
      logger.info "status: #{st.exitstatus || 'nil'} (#{st})"
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
  end

end
