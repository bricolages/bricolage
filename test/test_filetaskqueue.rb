require 'test/unit'
require 'bricolage/context'
require 'bricolage/jobnet'
require 'bricolage/taskqueue'
require 'pathname'

module Bricolage
  class TestFileTaskQueue < Test::Unit::TestCase
    exist_queue_path = Pathname.new('test/home/subsys/test_exist.queue')
    temp_queue_path = Pathname.new('test/home/subsys/test_temp.queue')
    temp_lock_path = Pathname.new('test/home/subsys/test_temp.queue.LOCK')

    context = Context.for_application(home_path='test/home')
    jobnet_path = Pathname.new('test/home/subsys/net1.jobnet')
    jobnet = RootJobNet.load_auto(context, [jobnet_path])

    teardown do
      temp_queue_path.delete if temp_queue_path.exist?
      temp_lock_path.delete if temp_lock_path.exist?
    end

    test "#restore_jobnet" do
      queue = FileTaskQueue.new(path: temp_queue_path)
      queue.restore_jobnet(jobnet)
      assert_equal 0, queue.size

      queue = FileTaskQueue.new(path: exist_queue_path)
      queue.restore_jobnet(jobnet)
      assert_equal 1, queue.size
    end

    test "#enqueue_jobnet" do
      queue = FileTaskQueue.new(path: temp_queue_path)

      queue.restore_jobnet(jobnet)
      assert_equal 0, queue.size

      queue.enqueue_jobnet(jobnet)
      assert_equal 4, queue.size

      jobs = []
      queue.each do |job|
        jobs.push job
      end
      assert_equal ['subsys', 'job1'], [jobs[0].subsystem, jobs[0].name]
      assert_equal ['subsys', 'job4'], [jobs[3].subsystem, jobs[3].name]
    end

    class DummyJobResult
      def success?
        true
      end
    end

    test "#consume_each" do
      queue = FileTaskQueue.new(path: temp_queue_path)
      queue.enqueue_jobnet(jobnet)
      assert_false queue.locked?(jobnet)
      assert_equal 4, queue.size
      jobs = []
      queue.consume_each do |job|
        assert_true queue.locked?(jobnet)
        assert_equal 4 - jobs.size, File.readlines(temp_queue_path).size
        assert_equal 4 - jobs.size, queue.size
        jobs.push job
        DummyJobResult.new
      end
      assert_false queue.locked?(jobnet)
      assert_equal 0, queue.size
      assert_equal ['subsys', 'job1'], [jobs[0].subsystem, jobs[0].name]
      assert_equal ['subsys', 'job2'], [jobs[1].subsystem, jobs[1].name]
      assert_equal ['subsys', 'job3'], [jobs[2].subsystem, jobs[2].name]
      assert_equal ['subsys', 'job4'], [jobs[3].subsystem, jobs[3].name]
    end
  end
end
