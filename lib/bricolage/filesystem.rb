require 'bricolage/resource'
require 'bricolage/exception'
require 'pathname'

module Bricolage

  class FileSystem
    def FileSystem.for_option_pathes(home_path, job_path, env)
      if job_path
        home, subsys_id = extract_home_dirs(job_path)
        if home_path and home_path.realpath.to_s != home.realpath.to_s
          raise OptionError, "--home option and job file is exclusive"
        end
        new(home, env).subsystem(subsys_id)
      elsif home_path
        new(home_path, env)
      elsif home = ENV['BRICOLAGE_HOME']
        new(home, env)
      else
        new(Pathname.getwd, env)
      end
    end

    # job_path -> [home_path, subsys_id]
    def FileSystem.extract_home_dirs(job_path)
      subsys_path = Pathname(job_path).realpath.parent
      return subsys_path.parent, subsys_path.basename
    rescue SystemCallError => err
      raise ParameterError, "failed to access job file: #{err.message}"
    end

    def initialize(path, env)
      @path = Pathname(path)
      @environment = env
      @subsystems = {}
    end

    def scoped?
      false
    end

    attr_reader :path
    attr_reader :environment

    def inspect
      "\#<#{self.class} #{@path}>"
    end

    def root
      self
    end

    def subsystem(id)
      @subsystems[id.to_s] ||= begin
        unless root.relative(id).directory?
          raise ParameterError, "no such subsystem: #{id}"
        end
        ScopedFileSystem.new(root, id)
      end
    end

    def subsystems
      root.subdirectories
          .map {|path| path.basename.to_s }
          .select {|name| /\A\w+\z/ =~ name }
          .reject {|name| name == 'config' }
          .map {|name| subsystem(name) }
    end

    def subdirectories
      @path.children(true).select {|path| path.directory? }
    end

    def home_path
      root.path
    end

    def job_dir
      scoped? ? @path : nil
    end

    def exist?(name)
      relative(name).exist?
    end

    def file(name)
      FileResource.new(relative(name))
    end

    def root_relative_path(rel)
      root.relative_path(rel)
    end

    alias root_relative root_relative_path

    def relative_path(name)
      path = Pathname(name)
      if path.absolute?
        path
      else
        @path + path
      end
    end

    alias relative relative_path

    # typed_name("make_master", "sql") -> Pathname("$prefix/make_master.sql")
    def typed_name(name, type)
      relative(name.count('.') > 0 ? name : "#{name}.#{type}")
    end

    # typed_file("make_master", "sql") -> FileResource("$prefix/make_master.sql")
    def typed_file(name, type)
      FileResource.new(typed_name(name, type))
    end

    def parameter_file(name, type)
      name.count('/') == 0 ? typed_file(name, type) : root.file(name)
    end

    def parameter_file_loader
      ConfigLoader.new(home_path)
    end

    def config_pathes(name)
      [ "config/#{name}", "config/#{@environment}/#{name}" ].map {|rel| root.relative(rel) }
    end

    def config_file_loader
      ConfigLoader.new(home_path)
    end

    def job_file(id)
      path = typed_name(id, 'job')
      return path if path.exist?
      glob("#{id}.*.job").first or path
    end

    def glob(pattern)
      Dir.glob("#{@path}/#{pattern}").map {|path| Pathname(path) }
    end

    ## */*.TYPE
    def all_typed_pathes(type)
      subsystems.map {|subsys| subsys.typed_pathes(type) }.flatten
    end

    def typed_pathes(type)
      @path.children.select {|path| path.file? and path.extname.to_s == ".#{type}" }
    end
  end

  class ScopedFileSystem < FileSystem
    def initialize(parent, id)
      super parent.relative(id), parent.environment
      @parent = parent
      @id = id
    end

    def scoped?
      true
    end

    def root
      @parent.root
    end
  end

end
