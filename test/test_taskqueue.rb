require 'test/unit'
require 'bricolage/context'
require 'bricolage/job'
require 'bricolage/jobnet'
require 'bricolage/taskqueue'
require 'pathname'
require 'pp'

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
      queue.enqueue JobTask.deserialize('subsys/test_dummy_job_task')
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
    datasource = context.get_data_source('psql', 'test_db')
    jobnet_path = Pathname.new('test/home/subsys/net1.jobnet')
    jobnet_ref = RootJobNet.load_auto(context, [jobnet_path]).jobnets.first
    jobrefs = jobnet_ref.refs - [jobnet_ref.start, *jobnet_ref.net_refs, jobnet_ref.end]

    teardown do
      queue = DatabaseTaskQueue.new(datasource, 'subsys', 'net1', jobrefs, 'dummy_executor')
      queue.clear
    end

    test "#enqueue/#dequeuing/#dequeued" do
      queue = DatabaseTaskQueue.new(datasource, 'subsys', 'net1', jobrefs, 'dummy_executor')
      assert_equal 0, queue.size
      queue.enqueue_job_executions
      assert_equal 2, queue.size
      queue.dequeuing
      assert_equal 2, queue.size
      e = queue.dequeued
      assert_equal 1, queue.size
      queue.enqueue e.first
      assert_equal 2, queue.size
    end

    test "DatabaseTaskQueue.restore_if_exist" do
      queue1 = DatabaseTaskQueue.restore_if_exist(datasource, jobnet_ref, 'dummy_executor')
      assert_equal 2, queue1.size
      queue1.dequeuing
      queue1.dequeued
      queue2 = DatabaseTaskQueue.restore_if_exist(datasource, jobnet_ref, 'dummy_executor')
      assert_equal 1, queue2.size
    end

    test "#lock_jobnet/#unlock_jobnet" do
      queue = DatabaseTaskQueue.restore_if_exist(datasource, jobnet_ref, 'dummy_executor')
      assert_false  queue.locked?
      queue.lock_jobnet
      assert_true  queue.locked?
      queue.unlock_jobnet
      assert_false queue.locked?
    end

    test "#lock_job/#unlock_job" do
      queue = DatabaseTaskQueue.restore_if_exist(datasource, jobnet_ref, 'dummy_executor')
      assert_false queue.locked?
      task = queue.next
      queue.lock_job(task)
      assert_true  queue.locked?
      queue.unlock_job(task)
      assert_false queue.locked?
    end
  end
end
