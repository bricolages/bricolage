require_relative 'lib/bricolage/version'

Gem::Specification.new do |s|
  s.platform = Gem::Platform::RUBY
  s.name = 'bricolage'
  s.version = Bricolage::VERSION
  s.summary = 'SQL Batch Framework'
  s.description = 'Redshift-oriented Data Warehouse Batch Framework'
  s.license = 'MIT'

  s.author = ['Minero Aoki']
  s.email = 'aamine@loveruby.net'
  s.homepage = 'https://github.com/aamine/bricolage'

  s.executables = Dir.entries('bin').select {|ent| File.file?("bin/#{ent}") }
  s.files = Dir.glob(['README.md', 'bin/*', 'lib/**/*.rb', 'libexec/**/*', 'jobclass/*.rb', 'test/**/*'])
  s.require_path = 'lib'

  s.required_ruby_version = '>= 2.0.0'
  s.add_dependency 'pg', '~> 0.18.0'
  s.add_dependency 'aws-sdk-s3', '~> 1'
  s.add_dependency 'aws-sdk-sns', '~> 1'
  s.add_dependency 'redis', ">= 3.0.0"
  s.add_development_dependency 'test-unit'
  s.add_development_dependency 'pry'
  s.add_development_dependency 'rake'
  s.add_development_dependency 'mocha'
end
