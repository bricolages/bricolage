require 'test/unit'
require 'bricolage/context'
require 'bricolage/job'
require 'bricolage/jobnet'
require 'bricolage/taskqueue'
require 'pathname'
require 'pp'
require 'pry'

module Bricolage
  class TestFileTaskQueue < Test::Unit::TestCase
    exist_queue_path = Pathname.new('test/home/subsys/test_exist.queue')
    temp_queue_path = Pathname.new('test/home/subsys/test_temp.queue')
    temp_lock_path = Pathname.new('test/home/subsys/test_temp.queue.LOCK')

    teardown do
      temp_queue_path.delete if temp_queue_path.exist?
      temp_lock_path.delete if temp_lock_path.exist?
    end

    test "FileTaskQueue.restore_if_exist" do
      queue = FileTaskQueue.restore_if_exist(temp_queue_path)
      assert_equal 0, queue.size
      queue = FileTaskQueue.restore_if_exist(exist_queue_path)
      assert_equal 1, queue.size
    end

    test "#save" do
      queue = FileTaskQueue.restore_if_exist(temp_queue_path)
      assert_false queue.queued?
      queue.enq JobTask.new('test_dummy_job_task')
      queue.save
      assert_true  queue.queued?
    end

    test "#lock" do
      queue = FileTaskQueue.restore_if_exist(temp_queue_path)
      assert_false queue.locked?
      queue.lock
      assert_true  queue.locked?
    end

    test "#unlock" do
      queue = FileTaskQueue.restore_if_exist(temp_queue_path)
      queue.lock
      assert_true  queue.locked?
      queue.unlock
      assert_false queue.locked?
    end
  end

  class TestDatabaseTaskQueue < Test::Unit::TestCase
    context = Context.for_application(home_path='test/home')
    jobnet_path = Pathname.new('test/home/subsys/net1.jobnet')
    jobnet = RootJobNet.load_auto(context, [jobnet_path]).jobnets.first
    jobrefs = jobnet.refs - [jobnet.start, *jobnet.net_refs, jobnet.end]
    jobtask1 = JobTask.new(jobrefs.pop)
    jobtask2 = JobTask.new(jobrefs.pop)

    teardown do
      queue = DatabaseTaskQueue.restore_if_exist(context, jobnet)
      queue.unlock
      queue.clear
    end

    test "DatabaseTaskQueue.restore_if_exist" do
      queue1 = DatabaseTaskQueue.restore_if_exist(context, jobnet)
      assert_equal 0, queue1.size
      queue1.enq jobtask1
      queue1.enq jobtask2
      queue1.save

      queue2 = DatabaseTaskQueue.restore_if_exist(context, jobnet)
      assert_equal 2, queue2.size
    end

    test "#save" do
      queue = DatabaseTaskQueue.restore_if_exist(context, jobnet)
      assert_false queue.queued?
      queue.enq jobtask1
      queue.save
      assert_true  queue.queued?
    end

    test "#lock" do
      queue = DatabaseTaskQueue.restore_if_exist(context, jobnet)
      queue.enq jobtask1
      queue.save
      assert_false queue.locked?
      queue.lock
      assert_true  queue.locked?
    end

    test "#unlock" do
      queue = DatabaseTaskQueue.restore_if_exist(context, jobnet)
      queue.enq jobtask1
      queue.save
      queue.lock
      assert_true  queue.locked?
      queue.unlock
      assert_false queue.locked?
    end
  end
end
