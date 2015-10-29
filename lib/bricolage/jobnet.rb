require 'bricolage/exception'
require 'tsort'

module Bricolage

  # Represents "first" jobnet given by command line (e.g. bricolage-jobnet some.jobnet)
  class RootJobNet
    def RootJobNet.load(ctx, path)
      root = new(JobNet::FileLoader.new(ctx), JobNet.load(path))
      root.load_recursive
      root.fix
      root
    end

    def initialize(jobnet_loader, start_jobnet)
      @jobnet_loader = jobnet_loader
      @start_jobnet = start_jobnet
      @jobnets = {start_jobnet.ref => start_jobnet}
      @graph = nil
    end

    attr_reader :start_jobnet

    def each_jobnet(&block)
      @jobnets.each_value(&block)
    end

    def jobnets
      @jobnets.values
    end

    def load_recursive
      unresolved = [@start_jobnet]
      until unresolved.empty?
        loaded = []
        unresolved.each do |net|
          net.net_refs.each do |ref|
            next if ref.jobnet
            unless net = @jobnets[ref]
              net = @jobnet_loader.load(ref)
              @jobnets[ref] = net
              loaded.push net
            end
            ref.jobnet = net
          end
        end
        unresolved = loaded
      end
    end

    def fix
      each_jobnet do |net|
        net.fix
      end
      @jobnets.freeze
      @dag = JobDAG.build(jobnets)
    end

    def sequential_jobs
      @dag.sequential_jobs
    end
  end

  class JobDAG
    def JobDAG.build(jobnets)
      graph = new
      jobnets.each do |net|
        graph.merge! net
      end
      graph.fix
      graph
    end

    def initialize
      @deps = Hash.new { Array.new }   # {JobRef => [JobRef]} (dest->srcs)
    end

    def to_hash
      h = {}
      @deps.each do |dest, srcs|
        h[dest.to_s] = srcs.map(&:to_s)
      end
      h
    end

    def merge!(net)
      net.each_dependencies do |ref, deps|
        @deps[ref] |= deps
      end
    end

    def fix
      @deps.freeze
      check_cycle
      check_orphan
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

    def check_cycle
      each_strongly_connected_component do |refs|
        unless refs.size == 1
          cycle = (refs + [refs.first]).reverse.join(' -> ')
          raise ParameterError, "found cycle in the jobnet: #{cycle}"
        end
      end
    end

    def check_orphan
      orphan_nodes.each do |ref|
        raise ParameterError, "found orphan job in the jobnet: #{ref.location}: #{ref}"
      end
    end

    def orphan_nodes
      @deps.to_a.select {|ref, deps| deps.empty? and not ref.dummy? }.map {|ref, *| ref }
    end
  end

  class JobNet
    def JobNet.load(path, ref = JobNetRef.for_path(path))
      File.open(path) {|f|
        Parser.new(ref).parse_stream(f)
      }
    rescue SystemCallError => err
      raise ParameterError, "could not load jobnet: #{path} (#{err.message})"
    end

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
        h[src.to_s] = dests.map(&:to_s)
      end
      h
    end

    def to_deps_hash
      h = {}
      @deps.each do |dest, srcs|
        h[dest.to_s] = srcs.map(&:to_s)
      end
      h
    end

    def refs
      @flow.keys | @flow.values.flatten
    end

    def net_refs
      @deps.keys.select {|ref| ref.net? }
    end

    # Adds dummy dependencies (@start and @end) to fix up all jobs
    # into one DAG beginning with @start and ending with @end
    def fix
      refs.each do |ref|
        next if ref.dummy?
        unless @deps[ref]
          (@flow[self.start] ||= []).push ref
          @deps[ref] = [self.start]
        end
        unless @flow[ref]
          (@flow[ref] ||= []).push self.end
          (@deps[self.end] ||= []).push ref
        end
      end
      @deps[self.start] ||= []
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

  class JobNet   # reopen as namespace

    class FileLoader
      def initialize(ctx)
        @context = ctx
      end

      def load(ref)
        path = @context.root_relative_path(ref.relative_path)
        raise ParameterError, "undefined subnet: #{ref}" unless path.file?
        JobNet.load(path, ref)
      end
    end

    class Parser
      def initialize(jobnet_ref)
        @jobnet_ref = jobnet_ref
      end

      def parse_stream(f)
        net = JobNet.new(@jobnet_ref, Location.for_file(f))
        foreach_edge(f) do |src, dest|
          net.add_edge src, dest
        end
        net
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
      def JobNetRef.for_path(path)
        new(path.parent.basename, path.basename('.jobnet'), Location.dummy)
      end

      def initialize(subsys, name, location)
        super
        @jobnet = nil
        @start = nil
        @end = nil
      end

      attr_accessor :jobnet

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
        @jobnet.start
      end

      def end
        @jobnet.end
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
