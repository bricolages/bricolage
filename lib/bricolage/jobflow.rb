require 'bricolage/exception'
require 'tsort'

module Bricolage

  class JobFlow
    def JobFlow.load(path)
      File.open(path) {|f|
        parse_stream(f, make_node_ref(path))
      }
    rescue SystemCallError => err
      raise ParameterError, "could not load job flow: #{path} (#{err.message})"
    end

    def JobFlow.make_node_ref(path)
      subsys = path.parent.basename.to_s
      name = path.basename('.jobnet').to_s
      JobNetRef.new(subsys, name, Location.dummy)
    end

    def JobFlow.parse_stream(f, ref)
      Parser.new(ref.subsystem).parse_stream(f, ref)
    end

    def JobFlow.root
      RootJobFlow.new
    end

    ROOT_FLOW_NAME = '*'

    def initialize(ref, location)
      @ref = ref
      @location = location
      @flow = {}   # Ref => [Ref] (src->dest)
      @deps = {}   # Ref => [Ref] (dest->src)
      @subnets_resolved = false
    end

    def inspect
      "\#<#{self.class} #{ref}>"
    end

    attr_reader :ref

    def name
      ref.to_s
    end

    def add_edge(src, dest)
      (@flow[src] ||= []).push dest unless dest.net?
      (@deps[dest] ||= []).push src
    end

    def flow_tree
      h = {}
      @flow.each do |src, dests|
        h[src.to_s] = dests.map {|d| d.to_s }
      end
      h
    end

    def dependencies
      h = {}
      @deps.each do |ref, deps|
        h[ref.to_s] = deps.map(&:to_s)
      end
      h
    end

    def dependent_flows
      @deps.values.flatten.select {|ref| ref.net? }.uniq
    end

    def sequential_nodes
      tsort.reject {|ref| ref.dummy? }
    end

    def sequential_jobs
      sequential_nodes.reject {|ref| ref.net? }
    end

    include TSort

    def tsort_each_node(&block)
      @deps.each_key(&block)
    end

    def tsort_each_child(ref, &block)
      @deps.fetch(ref).each(&block)
    end

    def fix_graph
      close_graph
      check_cycle
      check_orphan
    end

    def resolve_subnets(root_flow)
      @deps.each_key do |ref|
        next unless ref.net?
        ref.flow = root_flow.subnet(ref)
      end
      @subnets_resolved = true
    end

    def subnets_resolved?
      @subnets_resolved
    end

    private

    def close_graph
      @deps.values.flatten.uniq.each do |ref|
        @deps[ref] ||= []
      end
    end

    def check_cycle
      each_strongly_connected_component do |refs|
        unless refs.size == 1
          cycle = (refs + [refs.first]).reverse.join(' -> ')
          raise ParameterError, "found cycle in the flow: #{cycle}"
        end
      end
    end

    def check_orphan
      orphan_nodes.each do |ref|
        raise ParameterError, "found orphan job in the flow: #{ref.location}: #{ref}"
      end
    end

    def orphan_nodes
      @deps.to_a.select {|ref, deps| deps.empty? and not ref.dummy? and not ref.net? }.map {|ref, *| ref }
    end
  end

  class RootJobFlow < JobFlow
    def RootJobFlow.load(ctx, path)
      flow = new(ctx)
      flow.add_subnet JobFlow.load(path)
      flow.fix
      flow
    end

    def initialize(ctx)
      @ctx = ctx
      super ROOT_FLOW_NAME, Location.dummy
      @subnets = {}
    end

    def add_subnet(flow)
      raise ParameterError, "duplicated subnet definition: #{flow.name}" if @subnets.key?(flow.name)
      @subnets[flow.name] = flow
    end

    def subnet(ref)
      if flow = @subnets[ref]
        flow
      else
        flow = load_jobnet_auto(ref)
        add_subnet flow
        flow
      end
    end

    def load_jobnet_auto(ref)
      path = @ctx.root_relative_path(ref.relative_path)
      raise ParameterError, "undefined subnet: #{ref}" unless path.file?
      JobFlow.load(path)
    end

    def each_subnet(&block)
      @subnets.values.each(&block)
    end

    def each_flow(&block)
      yield self
      @subnets.each_value(&block)
    end

    def each_subnet_sequence
      sequential_nodes.each do |ref|
        yield subnet(ref)
      end
    end

    def fix
      resolve_subnet_references
      each_subnet do |flow|
        flow.fix_graph
      end
      add_jobnet_dependency_edges
      fix_graph
    end

    private

    def resolve_subnet_references
      unresolved = true
      while unresolved
        unresolved = false
        ([self] + @subnets.values).each do |flow|
          unless flow.subnets_resolved?
            flow.resolve_subnets self
            unresolved = true
          end
        end
      end
    end

    def add_jobnet_dependency_edges
      each_subnet do |flow|
        # dummy dependency to ensure to execute all subnets
        add_edge Ref.dummy, flow.ref
        # jobnet -> jobnet dependency
        flow.dependent_flows.each do |dep_flow_ref|
          add_edge dep_flow_ref, flow.ref
        end
      end
    end

    def check_orphan
      # should not check orphan for root.
    end
  end

  class JobFlow   # reopen as namespace

    class Ref
      START_NAME = '@start'

      def Ref.dummy
        JobRef.new(nil, START_NAME, Location.dummy)
      end

      def Ref.parse(ref, curr_subsys = nil, location = Location.dummy)
        return JobNetRef.new(nil, '', location) if ref == ROOT_FLOW_NAME
        m = %r<\A(\*)?(?:(\w[\w\-]*)/)?(@?\w[\w\-]*)\z>.match(ref) or
            raise ParameterError, "bad job name: #{ref.inspect}"
        is_net, subsys, name = m.captures
        ref_class = (is_net ? JobNetRef : JobRef)
        node_subsys = subsys || curr_subsys
        unless node_subsys
          raise ParameterError, "missing subsystem: #{ref}"
        end
        ref_class.new(node_subsys, name, location)
      end

      def initialize(subsys, name, location)
        @subsystem = subsys
        @name = name
        @location = location
      end

      attr_reader :subsystem
      attr_reader :name
      attr_reader :location

      def inspect
        "\#<#{self.class} #{to_s}>"
      end

      def to_s
        @ref ||= [@subsystem, @name].compact.join('/')
      end

      def ==(other)
        to_s == other.to_s
      end

      alias eql? ==

      def hash
        to_s.hash
      end

      def dummy?
        @name[0] == '@'
      end
    end

    class JobRef < Ref
      def net?
        false
      end
    end

    class JobNetRef < Ref
      def initialize(subsys, name, location)
        super
        @flow = nil
      end

      def flow=(flow)
        @flow = flow
      end

      def net?
        true
      end

      def to_s
        '*' + super
      end

      def relative_path
        "#{subsystem}/#{name}.jobnet"
      end
    end

    class Location
      def Location.dummy
        new('(dummy)', 0)
      end

      def Location.for_file(f)
        new(f.path, f.lineno)
      end

      def initialize(file, lineno)
        @file = file
        @lineno = lineno
      end

      attr_reader :file
      attr_reader :lineno

      def inspect
        "\#<#{self.class} #{to_s}>"
      end

      def to_s
        "#{@file}:#{@lineno}"
      end
    end

    class Parser
      def initialize(subsys)
        @subsys = subsys
      end

      def parse_stream(f, ref)
        flow = JobFlow.new(ref, Location.for_file(f))
        foreach_edge(f) do |src, dest|
          flow.add_edge src, dest
        end
        flow
      end

      private

      name = /\w[\w\-]*/
      node_ref = %r<[@*]?(?:#{name}/)?#{name}>
      START_PATTERN = /\A(#{node_ref})\z/
      DEPEND_PATTERN = /\A(#{node_ref})?\s*->\s*(#{node_ref})\z/

      def foreach_edge(f)
        default_src = Ref.dummy
        f.each do |line|
          text = line.sub(/\#.*/, '').strip
          next if text.empty?
          loc = Location.for_file(f)

          if m = DEPEND_PATTERN.match(text)
            src = m[1] ? ref(m[1], loc) : default_src
            dest = ref(m[2], loc)
            yield src, dest
            default_src = dest

          elsif m = START_PATTERN.match(text)
            dest = ref(m[1], loc)
            yield Ref.dummy, dest
            default_src = dest

          else
            raise ParameterError, "syntax error at #{loc}: #{line.strip.inspect}"
          end
        end
      end

      def ref(ref_str, location)
        Ref.parse(ref_str, @subsys, location)
      end
    end

  end

end
