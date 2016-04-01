module Bricolage

  module StreamingLoad

    class ManifestFile

      def ManifestFile.create(ds, job_id:, object_urls:, logger:, noop: false, &block)
        manifest = new(ds, job_id, object_urls, logger: logger, noop: noop)
        manifest.create_temporary(&block)
      end

      def initialize(ds, job_id, object_urls, logger:, noop: false)
        @ds = ds
        @job_id = job_id
        @object_urls = object_urls
        @logger = logger
        @noop = noop
      end

      def credential_string
        @ds.credential_string
      end

      def name
        @name ||= "manifest-#{@job_id}.json"
      end

      def url
        @url ||= @ds.url(name)
      end

      def content
        @content ||= begin
          ents = @object_urls.map {|url|
            { "url" => url, "mandatory" => true }
          }
          obj = { "entries" => ents }
          JSON.pretty_generate(obj)
        end
      end

      def put
        @logger.info "s3: put: #{url}"
        @ds.object(name).put(body: content) unless @noop
      end

      def delete
        @logger.info "s3: delete: #{url}"
        @ds.object(name).delete unless @noop
      end

      def create_temporary
        put
        yield self
        delete
      end

    end

  end

end
