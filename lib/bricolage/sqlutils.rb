module Bricolage

  module SQLUtils

    private

    def sql_string_literal(s)
      %Q('#{escape_sql_string s}')
    end

    alias s sql_string_literal

    def escape_sql_string(s)
      s.gsub(/'/, "''")
    end

    def sql_timestamp_literal(time)
      %Q(timestamp '#{sql_timestamp_format(time)}')
    end

    alias t sql_timestamp_literal

    def sql_timestamp_format(time)
      time.strftime('%Y-%m-%d %H:%M:%S')
    end

  end

end
