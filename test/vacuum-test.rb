$LOAD_PATH.unshift '../../lib'
require 'bricolage/psqldatasource'

ds = Bricolage::PSQLDataSource.new(port: 5444, database: 'production', username: 'aamine', pgpass: "#{ENV['HOME']}/.pgpass")
ds.__send__ :initialize_base, 'tmp', nil, Logger.new($stderr)
p ds
ds.vacuum 's'
puts 'OK'
