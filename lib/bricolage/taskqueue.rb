require 'bricolage/jobnet'
require 'bricolage/exception'
require 'pathname'

module Bricolage

  class TaskQueue
    def initialize
      @queue = []
    end

    def empty?
      @queue.empty?
    end

    def size
      @queue.size
    end

    def queued?
      not empty?
    end

    def each(&block)
      @queue.each(&block)
    end

    def consume_each
      lock
      save
      while task = self.next
        yield task
        deq
      end
    ensure
      unlock
    end

    def enq(task)
      @queue.push task
    end

    def next
      @queue.first
    end

    def deq
      task = @queue.shift
      save
      task
    end

    def save
    end

    def restore
    end

    def locked?
      false
    end

    def lock
    end

    def unlock
    end

    def unlock_help
      "[MUST NOT HAPPEN] this message must not be shown"
    end
  end

  class FileTaskQueue < TaskQueue
    def FileTaskQueue.restore_if_exist(path)
      q = new(path)
      q.restore if q.queued?
      q
    end

    def initialize(path)
      super()
      @path = path
    end

    def queued?
      @path.exist?
    end

    def save
      if empty?
        @path.unlink if @path.exist?
        return
      end
      tmpname = "#{@path}.tmp.#{Process.pid}"
      begin
        File.open(tmpname, 'w') {|f|
          each do |task|
            f.puts task.serialize
          end
        }
        File.rename tmpname, @path
      ensure
        FileUtils.rm_f tmpname
      end
    end

    def restore
      File.foreach(@path) do |line|
        enq JobTask.deserialize(line)
      end
    end

    def locked?
      lock_file_path.exist?
    end

    def lock
      FileUtils.touch lock_file_path
    end

    def unlock
      FileUtils.rm_f lock_file_path
    end

    def lock_file_path
      Pathname.new("#{@path}.LOCK")
    end

    def unlock_help
      "remove the file: #{lock_file_path}"
    end
  end

  class JobTask
    def initialize(job)
      @job = job
    end

    attr_reader :job

    def serialize
      [@job].join("\t")
    end

    def JobTask.deserialize(str)
      job, * = str.strip.split("\t")
      new(JobNet::Ref.parse(job))
    end
  end

end
