require 'bricolage/exception'
require 'tsort'

module Bricolage

  # Represents "first" jobnet given by command line (e.g. bricolage-jobnet some.jobnet)
  class RootJobFlow
    def RootJobFlow.load(ctx, path)
      root = new(JobFlow::FileLoader.new(ctx), JobFlow.load(path))
      root.load_recursive
      root.fix
      root
    end

    def initialize(jobnet_loader, start_flow)
      @jobnet_loader = jobnet_loader
      @start_flow = start_flow
      @flows = {start_flow.ref => start_flow}
      @graph = nil
    end

    attr_reader :start_flow

    def each_flow(&block)
      @flows.each_value(&block)
    end

    def load_recursive
      unresolved = [@start_flow]
      until unresolved.empty?
        loaded = []
        unresolved.each do |flow|
          flow.net_refs.each do |ref|
            next if ref.flow
            unless flow = @flows[flow.ref]
              flow = @jobnet_loader.load(ref)
              @flows[flow.ref] = flow
              loaded.push flow
            end
            ref.flow = flow
          end
        end
        unresolved = loaded
      end
    end

    def fix
      each_flow do |flow|
        flow.fix
      end
      @flows.freeze
      @graph = DependencyGraph.build(@flows.values)
    end

    def sequential_jobs
      @graph.sequential_jobs
    end
  end

  class DependencyGraph
    def DependencyGraph.build(flows)
      graph = new
      flows.each do |flow|
        graph.merge! flow
      end
      graph.fix
      graph
    end

    def initialize
      @deps = {}
    end

    def merge!(flow)
      flow.each_dependencies do |ref, deps|
        @deps[ref] = ((@deps[ref] || []) + deps).uniq
      end
      @deps[flow.start] ||= []
    end

    def fix
      @deps.freeze
      check_cycle(deps)
      check_orphan(deps)
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
    def initialize(ref, location)
      @ref = ref
      @location = location
      @flow = {}   # Ref => [Ref] (src->dest)
      @deps = {}   # Ref => [Ref] (dest->src)
    end

    def inspect
      "\#<#{self.class} #{ref}>"
    end

    attr_reader :ref

    def start
      @ref.start_ref
    end

    def end
      @ref.end_ref
    end

    def name
      @ref.to_s
    end

    def add_edge(src, dest)
      (@flow[src] ||= []).push dest
      (@deps[dest] ||= []).push src
    end

    def to_hash
      h = {}
      @flow.each do |src, dests|
        h[src.to_s] = dests.map {|d| d.to_s }
      end
      h
    end

    def net_refs
      @deps.keys.select {|ref| ref.net? }
    end

    # Adds dummy dependencies (@start and @end) to group sub-net jobs
    def fix
      (@flow.keys + @flow.values.flatten).uniq.each do |ref|
        next if ref.dummy?
        unless @deps[ref]
          @deps[ref] = [self.start]
        end
        unless @flow[ref]
          (@deps[self.end] ||= []).push ref
        end
      end
      @flow.freeze
      @deps.freeze
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
        load_file(path, ref)
      end

      private

      def load_file(path, ref)
        File.open(path) {|f|
          Parser.new(ref).parse_stream(f)
        }
      rescue SystemCallError => err
        raise ParameterError, "could not load job flow: #{path} (#{err.message})"
      end
    end

    class Parser
      def initialize(jobnet_ref)
        @jobnet_ref = jobnet_ref
      end

      def parse_stream(f)
        flow = JobFlow.new(@jobnet_ref, Location.for_file(f))
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

    class Ref
      def Ref.parse(ref, curr_subsys = nil, location = Location.dummy)
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
        @start = nil
        @end = nil
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
        @start ||= JobRef.new(subsystem, "@#{name}@start", location)
      end

      def end_ref
        @end ||= JobRef.new(subsystem, "@#{name}@end", location)
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

  end

end
