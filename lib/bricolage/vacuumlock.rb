require 'bricolage/exception'
require 'fileutils'

module Bricolage

  module VacuumLock
    def enable_vacuum_lock?
      !!ENV['BRICOLAGE_VACUUM_LOCK']
    end
    module_function :enable_vacuum_lock?

    DEFAULT_VACUUM_LOCK_FILE = '/tmp/bricolage.vacuum.lock'
    DEFAULT_VACUUM_LOCK_TIMEOUT = 3600   # 60min

    def vacuum_lock_parameters
      return nil unless enable_vacuum_lock?
      path, tm = ENV['BRICOLAGE_VACUUM_LOCK'].split(':', 2)
      timeout = tm ? [tm.to_i, 1].max : DEFAULT_VACUUM_LOCK_TIMEOUT
      return path, timeout
    end
    module_function :vacuum_lock_parameters

    def psql_serialize_vacuum_begin
      if enable_vacuum_lock?
        path, timeout = vacuum_lock_parameters
        "\\! #{create_lockfile_cmd} #{path} #{timeout}"
      else
        ';'
      end
    end
    module_function :psql_serialize_vacuum_begin

    def psql_serialize_vacuum_end
      if enable_vacuum_lock?
        path, _timeout = vacuum_lock_parameters
        "\\! rm -f #{path}"
      else
        ';'
      end
    end
    module_function :psql_serialize_vacuum_end

    def create_lockfile_cmd
      Pathname(__FILE__).parent.parent.parent + 'libexec/create-lockfile'
    end
    module_function :create_lockfile_cmd

    def serialize_vacuum
      return yield unless enable_vacuum_lock?
      path, timeout = vacuum_lock_parameters
      create_vacuum_lock_file path, timeout
      begin
        yield
      ensure
        FileUtils.rm_f path
      end
    end
    module_function :serialize_vacuum

    def create_vacuum_lock_file(path, timeout)
      start_time = Time.now
      begin
        File.open(path, File::WRONLY | File::CREAT | File::EXCL) {|f|
          f.puts "#{Time.now}: created by bricolage [#{Process.pid}]"
        }
      rescue Errno::EEXIST
        if Time.now - start_time > timeout
          raise LockTimeout, "could not create lock file: #{path} (timeout #{timeout} seconds)"
        end
        sleep 1
        retry
      end
    end
    module_function :create_vacuum_lock_file

    def VacuumLock.using
      return yield unless enable_vacuum_lock?
      begin
        yield
      ensure
        cleanup_vacuum_lock
      end
    end

    def using_vacuum_lock(&block)
      VacuumLock.using(&block)
    end

    def VacuumLock.cleanup_vacuum_lock
      return unless enable_vacuum_lock?
      path, _timeout = vacuum_lock_parameters
      if locking?(path)
        $stderr.puts "remove VACUUM lock by #{Process.pid}"
        FileUtils.rm_f path
      end
    end

    def VacuumLock.locking?(path)
      # do not check file existance, just read to avoid race condition
      locker_pid = File.read(path).slice(/\[(\d+)\]/, 1).to_i
      $stderr.puts "bricolage_pid: #{$$}, vacuum_locked_by: #{locker_pid}"
      locker_pid == Process.pid
    rescue
      $stderr.puts "bricolage_pid: #{$$}, vacuum_locked_by: (none)"
      false
    end
  end

end
