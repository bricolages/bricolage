require 'test/unit'
require 'bricolage/context'
require 'bricolage/job'
require 'bricolage/jobnet'
require 'bricolage/taskqueue'
require 'bricolage/dao/job'
require 'pathname'

module Bricolage
  class TestDatabaseTaskQueue < Test::Unit::TestCase
    context = Context.for_application(home_path='test/home')
    datasource = context.get_data_source('psql', 'test_db')
    job_execution_dao = DAO::JobExecution.new(datasource)
    jobnet_path = Pathname.new('test/home/subsys/net1.jobnet')
    jobnet = RootJobNet.load_auto(context, [jobnet_path])
    _jobrefs = jobnet.sequential_jobs

    jobnet_path2 = Pathname.new('test/home/subsys2/net.jobnet')
    jobnet2 = RootJobNet.load_auto(context, [jobnet_path2])

    teardown do
      job_execution_dao.delete_all
    end

    test "parse a nested jobnet" do
      queue = DatabaseTaskQueue.new(datasource: datasource, executor_id: 'dummy_executor')
      queue.enqueue_jobnet(jobnet)
      assert_equal 4, queue.size
    end

    test "parse a jobnet with another subsystem" do
      queue2 = DatabaseTaskQueue.new(datasource: datasource, executor_id: 'dummy_executor')
      queue2.enqueue_jobnet(jobnet2)
      assert_equal 7, queue2.size
    end

    class DummyJobResult
      def initialize(success = true)
        @success = success
      end

      def success?
        @success
      end

      def message
        'test job failed'
      end
    end

    test "#consume_each" do
      queue = DatabaseTaskQueue.new(datasource: datasource, executor_id: 'dummy_executor', enable_lock: true)
      queue.enqueue_jobnet(jobnet)
      assert_equal false, queue.locked?(jobnet)
      jobs = []
      queue.consume_each do |job|
        assert_equal true, queue.locked?(jobnet)
        assert_equal 4 - jobs.size, queue.size
        assert_equal 4 - jobs.size, job_execution_dao.enqueued_jobs(jobnet.ref).size
        jobs.push job
        DummyJobResult.new
      end
      assert_equal false, queue.locked?(jobnet)
      assert_equal 0, queue.size
      assert_equal ['subsys', 'job1'], [jobs[0].subsystem, jobs[0].name]
      assert_equal ['subsys', 'job2'], [jobs[1].subsystem, jobs[1].name]
      assert_equal ['subsys', 'job3'], [jobs[2].subsystem, jobs[2].name]
      assert_equal ['subsys', 'job4'], [jobs[3].subsystem, jobs[3].name]
    end

    test "#consume_each (no lock)" do
      queue = DatabaseTaskQueue.new(datasource: datasource, executor_id: 'dummy_executor', enable_lock: false)
      queue.enqueue_jobnet(jobnet)
      jobs = []
      queue.consume_each do |job|
        assert_equal 4 - jobs.size, queue.size
        assert_equal 4 - jobs.size, job_execution_dao.enqueued_jobs(jobnet.ref).size
        jobs.push job
        DummyJobResult.new
      end
      assert_equal 0, queue.size
    end

    test "#consume_each many times" do
      5.times do |seq|
        queue = DatabaseTaskQueue.new(datasource: datasource, executor_id: "t#{seq}", enable_lock: true)
        queue.enqueue_jobnet(jobnet)
        jobs = []
        queue.consume_each do |job|
          assert_equal 4 - jobs.size, queue.size
          assert_equal 4 - jobs.size, job_execution_dao.enqueued_jobs(jobnet.ref).size
          jobs.push job
          DummyJobResult.new
        end
        assert_equal 0, queue.size
      end
    end

    test "#cancel_jobnet" do
      queue = DatabaseTaskQueue.new(datasource: datasource, executor_id: 'dummy_executor', enable_lock: false)
      queue.enqueue_jobnet(jobnet)
      assert_equal 4, job_execution_dao.enqueued_jobs(jobnet.ref).size
      queue.cancel_jobnet(jobnet, 'test')
      assert_equal 0, job_execution_dao.enqueued_jobs(jobnet.ref).size
    end

    test "#consume_each (multiple jobnets mix)" do
      q1 = DatabaseTaskQueue.new(datasource: datasource, executor_id: 't1', enable_lock: true)
      q1.enqueue_jobnet(jobnet)
      q2 = DatabaseTaskQueue.new(datasource: datasource, executor_id: 't2', enable_lock: true)
      q2.enqueue_jobnet(jobnet2)
      assert_equal 4, job_execution_dao.enqueued_jobs(jobnet.ref).size
      assert_equal 7, job_execution_dao.enqueued_jobs(jobnet2.ref).size

      i = 0
      q1.consume_each do |job|
        i += 1
        DummyJobResult.new
      end
      assert_equal 4, i
      assert_equal 0, job_execution_dao.enqueued_jobs(jobnet.ref).size
      assert_equal 7, job_execution_dao.enqueued_jobs(jobnet2.ref).size

      j = 0
      q2.consume_each do |job|
        j += 1
        DummyJobResult.new
      end
      assert_equal 7, j
      assert_equal 0, job_execution_dao.enqueued_jobs(jobnet.ref).size
      assert_equal 0, job_execution_dao.enqueued_jobs(jobnet2.ref).size
    end

    test "#consume_each (multiple jobnets with cancellation)" do
      q1 = DatabaseTaskQueue.new(datasource: datasource, executor_id: 't1', enable_lock: true)
      q1.enqueue_jobnet(jobnet)
      q2 = DatabaseTaskQueue.new(datasource: datasource, executor_id: 't2', enable_lock: true)
      q2.enqueue_jobnet(jobnet2)
      assert_equal 4, job_execution_dao.enqueued_jobs(jobnet.ref).size
      assert_equal 7, job_execution_dao.enqueued_jobs(jobnet2.ref).size

      i = 0
      q1.consume_each do |job|
        i += 1
        DummyJobResult.new(i == 1)
      end
      assert_equal 3, job_execution_dao.enqueued_jobs(jobnet.ref).size

      q1.cancel_jobnet(jobnet, 'test')
      assert_equal 0, job_execution_dao.enqueued_jobs(jobnet.ref).size
      assert_equal 7, job_execution_dao.enqueued_jobs(jobnet2.ref).size

      j = 0
      q2.consume_each do |job|
        j += 1
        DummyJobResult.new
      end
      assert_equal 7, j
      assert_equal 0, job_execution_dao.enqueued_jobs(jobnet.ref).size
      assert_equal 0, job_execution_dao.enqueued_jobs(jobnet2.ref).size
    end

  end
end
