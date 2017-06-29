module Bricolage
  class S3Writer
    def initialize(ds, key)
      @ds = ds
      @key = key
    end

    def url
      @url ||= @ds.url(@key)
    end

    attr_reader :key

    def object
      @object ||= @ds.object(@key)
    end

    def upload(path)
      object.upload_file(path)
    end
  end
end
