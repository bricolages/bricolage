module Bricolage

  module StreamingLoad

    class URLPatternNotMatched < StandardError; end


    class URLPatterns

      def URLPatterns.for_config(configs)
        new(configs.map {|c|
          Pattern.new(url: c.fetch('url'), schema: c.fetch('schema'), table: c.fetch('table'))
        })
      end

      def initialize(patterns)
        @patterns = patterns
      end

      def match(url)
        @patterns.each do |pat|
          components = pat.match(url)
          return components if components
        end
        raise URLPatternNotMatched, "no URL pattern matches the object url: #{url.inspect}"
      end

      class Pattern
        def initialize(url:, schema:, table:)
          @url_pattern = /\A#{url}\z/
          @schema = schema
          @table = table
        end

        attr_reader :url_pattern
        attr_reader :schema
        attr_reader :table

        def match(url)
          m = @url_pattern.match(url) or return nil
          Components.new(get_component(m, @schema), get_component(m, @table))
        end

        def get_component(m, label)
          if /\A%/ =~ label
            m[label[1..-1]]
          else
            label
          end
        end
      end

      Components = Struct.new(:schema_name, :table_name)

    end

  end   # module StreamingLoad

end   # module Bricolage
