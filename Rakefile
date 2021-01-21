require 'rake/testtask'

task :test do
  desc 'Run tests'
  Rake::TestTask.new do |t|
    # To run test cases of specific file(s), Use:
    # % rake test TEST=test/test_specified_path.rb
    t.libs << "test"
    t.test_files = Dir["test/**/test_*.rb"]
    t.verbose = true
    t.warning = true
  end
end
