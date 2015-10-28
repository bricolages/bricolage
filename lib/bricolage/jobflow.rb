require 'bricolage/exception'
require 'tsort'

module Bricolage

  class RootJobFlow
    def RootJobFlow.load(ctx, path)
      flow = new(JobFlow::FileLoader.new(ctx), JobFlow.load(path))
      flow.fix
      flow
    end

    def initialize(jobnet_loader, start_flow)
      @jobnet_loader = jobnet_loader
      @start_flow = start_flow
      @flows = {start_flow.ref => start_flow}
      @deps = nil
    end

    def add_subnet(flow)
      raise ParameterError, "duplicated subnet definition: #{flow.name}" if @flows.key?(flow.name)
      @flows[flow.ref] = flow
    end

    def subnet(ref)
      unless flow = @flows[ref]
        flow = @jobnet_loader.load(ref)
        add_subnet flow
      end
      flow
    end

    def each_flow(&block)
      # Duplicates list to avoid modification in each
      @flows.values.each(&block)
    end

    def fix
      load_all_flows
      build_whole_flow
    end

    def sequential_jobs
      tsort.reject {|ref| ref.dummy? }
    end

    include TSort

    def tsort_each_node(&block)
      @deps.each_key(&block)
    end

    def tsort_each_child(ref, &block)
      @deps.fetch(ref).each(&block)
    end

    private

    def load_all_flows
      # RootJobNet#subnet loads jobflow lazily, we need to process jobnets recursively
      unresolved = true
      while unresolved
        unresolved = false
        each_flow do |flow|
          unless flow.subnets_resolved?
            flow.resolve_subnets self
            unresolved = true
          end
        end
      end
    end

    def build_whole_flow
      each_flow do |flow|
        flow.close
      end
      @deps = deps = build_deps
      check_cycle(deps)
      check_orphan(deps)
    end

    def build_deps
      h = {}
      each_flow do |flow|
        flow.each_dependencies do |ref, deps|
          h[ref] = ((h[ref] || []) + deps).uniq
        end
      end
      h[@start_flow.start] ||= []
      h
    end

    def check_cycle(deps)
      each_strongly_connected_component do |refs|
        unless refs.size == 1
          cycle = (refs + [refs.first]).reverse.join(' -> ')
          raise ParameterError, "found cycle in the flow: #{cycle}"
        end
      end
    end

    def check_orphan(deps)
      orphan_nodes(deps).each do |ref|
        raise ParameterError, "found orphan job in the flow: #{ref.location}: #{ref}"
      end
    end

    def orphan_nodes(deps)
      deps.to_a.select {|ref, deps| deps.empty? and not ref.dummy? and not ref.net? }.map {|ref, *| ref }
    end
  end

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
      Parser.new(ref).parse_stream(f, ref)
    end

    def JobFlow.root
      RootJobFlow.new
    end

    ROOT_FLOW_NAME = '*'

    def initialize(ref, location)
      @ref = ref
      @location = location
      @start = ref.start_ref
      @end = ref.end_ref
      @flow = {}   # Ref => [Ref] (src->dest)
      @deps = {}   # Ref => [Ref] (dest->src)
      @subnets_resolved = false
    end

    def inspect
      "\#<#{self.class} #{ref}>"
    end

    attr_reader :ref
    attr_reader :start
    attr_reader :end

    def name
      ref.to_s
    end

    def add_edge(src, dest)
      (@flow[src] ||= []).push dest
      (@deps[dest] ||= []).push src
    end

    def flow_tree
      h = {}
      @flow.each do |src, dests|
        h[src.to_s] = dests.map {|d| d.to_s }
      end
      h
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

    def close
      (@flow.keys + @flow.values.flatten).uniq.each do |ref|
        next if ref.dummy?
        unless @deps[ref]
          @deps[ref] = [@start]
        end
        unless @flow[ref]
          (@deps[@end] ||= []).push ref
        end
      end
    end

    def each_dependencies
      @deps.each do |ref, deps|
        dest = (ref.net? ? ref.start : ref)
        srcs = deps.map {|r| r.net? ? r.end : r }
        yield dest, srcs
      end
    end
  end

  class JobFlow   # reopen as namespace

    class FileLoader
      def initialize(ctx)
        @context = ctx
      end

      def load(ref)
        path = @context.root_relative_path(ref.relative_path)
        raise ParameterError, "undefined subnet: #{ref}" unless path.file?
        JobFlow.load(path)
      end
    end

    class Ref
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

      attr_accessor :flow

      def net?
        true
      end

      def to_s
        '*' + super
      end

      def relative_path
        "#{subsystem}/#{name}.jobnet"
      end

      def start_ref
        JobRef.new(subsystem, "@#{name}@start", location)
      end

      def end_ref
        JobRef.new(subsystem, "@#{name}@end", location)
      end


      def start
        @flow.start
      end

      def end
        @flow.end
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
      def initialize(jobnet_ref)
        @jobnet_ref = jobnet_ref
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
        default_src = nil
        f.each do |line|
          text = line.sub(/\#.*/, '').strip
          next if text.empty?
          loc = Location.for_file(f)

          if m = DEPEND_PATTERN.match(text)
            src = (m[1] ? ref(m[1], loc) : default_src) or
                raise ParameterError, "syntax error at #{loc}: '->' must follow any job"
            dest = ref(m[2], loc)
            yield src, dest
            default_src = dest

          elsif m = START_PATTERN.match(text)
            dest = ref(m[1], loc)
            yield @jobnet_ref.start_ref, dest
            default_src = dest

          else
            raise ParameterError, "syntax error at #{loc}: #{line.strip.inspect}"
          end
        end
      end

      def ref(ref_str, location)
        Ref.parse(ref_str, @jobnet_ref.subsystem, location)
      end
    end

  end

end
