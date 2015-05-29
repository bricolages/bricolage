require 'bricolage/exception'

module Bricolage

  class Resource
    def each_line(&block)
      content.each_line(&block)
    end
  end

  class FileResource < Resource
    def initialize(path, name = nil)
      @path = path
      @name = name
    end

    attr_reader :path

    def name
      @name || @path
    end

    def content
      @content ||= File.read(@path)
    rescue SystemCallError => err
      raise ParameterError, "could not open a file: #{@path}: #{err.message}"
    end

    def inspect
      "\#<#{self.class} #{@path}>"
    end

    def ==(other)
      return false unless other.kind_of?(FileResource)
      @path == other.path
    end

    alias eql? ==

    def hash
      @path.hash
    end
  end

  class StringResource < Resource
    def initialize(content, name = '(string)')
      @content = content
      @name = name
    end

    attr_reader :content
    attr_reader :name

    def inspect
      "\#<#{self.class} #{(@content.size > 20 ? @content[0, 20] : @content).inspect}>"
    end

    def ==(other)
      return false unless other.kind_of?(StringResource)
      @name == other.name and @content == other.content
    end

    def hash
      @name.hash ^ @content.hash
    end
  end

end
