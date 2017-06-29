require 'bricolage/loglocator'
require 'bricolage/s3writer'

module Bricolage
  class LogLocatorBuilder
    def LogLocatorBuilder.for_options(ctx, log_path_format, s3_ds, s3_key_format)
      ds = s3_ds ? ctx.get_data_source('s3', s3_ds) : nil
      new(log_path_format, ds, s3_key_format)
    end

    def initialize(log_path_format, s3_ds, s3_key_format)
      @log_path_format = log_path_format
      @s3_ds = s3_ds
      @s3_key_format = s3_key_format
    end

    def build(**params)
      path = @log_path_format ? @log_path_format.format(**params) : nil
      s3_writer =
        if @s3_ds and @s3_key_format
          key = @s3_key_format.format(**params)
          S3Writer.new(@s3_ds, key)
        else
          nil
        end
      LogLocator.new(path, s3_writer)
    end
  end
end
