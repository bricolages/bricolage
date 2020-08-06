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
  s.homepage = 'https://github.com/bricolages/bricolage'

  s.files = `git ls-files -z`.split("\x0").reject {|f| f.match(%r{^(test|spec|features)/}) }
  s.executables = s.files.grep(%r{bin/}).map {|path| File.basename(path) }
  s.require_path = 'lib'

  s.required_ruby_version = '>= 2.4.0'
  s.add_dependency 'pg', '~> 1.2.3'
  s.add_dependency 'aws-sdk-s3', '~> 1.64'
  s.add_dependency 'aws-sdk-sns', '~> 1.23'
  s.add_development_dependency 'test-unit', '~> 3.3'
  s.add_development_dependency 'rake', '~> 13.0'
  s.add_development_dependency 'mocha', '~> 1.11'
  s.add_development_dependency 'pry-byebug', '~> 3.9'
end
