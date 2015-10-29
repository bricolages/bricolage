require 'bricolage/jobflow'
require 'pathname'
require 'stringio'
require 'forwardable'
require 'pp'

class ConstantLoader
  def initialize(subnets)
    @subnets = subnets
  end

  def load(ref)
    @subnets[ref.to_s]
  end
end

def root_flow(subnets)
  h = {}
  subnets.each do |flow|
    h[flow.name] = flow
  end
  root = Bricolage::RootJobFlow.new(ConstantLoader.new(h))
  subnets.each do |flow|
    root.add_subnet flow
  end
  root.fix
  root
end

class StringFile
  def initialize(str, path)
    @f = StringIO.new(str)
    @path = path
  end

  attr_reader :path

  extend Forwardable
  def_delegators '@f', :each, :lineno
end

def make_flow(src, name)
  path = "test/#{name}.jobnet"
  ref = Bricolage::JobFlow.make_node_ref(Pathname(path))
  Bricolage::JobFlow.parse_stream(StringFile.new(src, path), ref)
end

net1 = make_flow(<<-End, 'net1')
job1
-> *net2
-> job4
End

net2 = make_flow(<<-End, 'net2')
job3
-> job4
End

root_flow([net1, net2]).each_subnet do |flow|
  pp flow.sequential_nodes
end
