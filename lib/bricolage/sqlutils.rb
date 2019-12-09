module Bricolage

  module SQLUtils

    private

    def sql_string_literal(s)
      %Q('#{escape_sql_string s}')
    end

    alias s sql_string_literal

    def escape_sql_string(s)
      s.gsub(/'/, "''").gsub(/\\/, '\\\\')
    end

    def sql_timestamp_literal(time)
      %Q(timestamp '#{sql_timestamp_format(time)}')
    end

    alias t sql_timestamp_literal

    def sql_timestamp_format(time)
      time.strftime('%Y-%m-%d %H:%M:%S')
    end

    def compile_set_expr(values_hash)
      columns = values_hash.keys.map(&:to_s).join(', ')
      values = values_hash.values.map{|v| convert_value(v) }.join(', ')
      return columns, values
    end

    def convert_value(value)
      if value == :now
        'now()'
      elsif value.nil?
        "null"
      elsif value == true or value == false
        "#{value.to_s}"
      elsif value.instance_of?(Integer) or value.instance_of?(Float)
        "#{value.to_s}"
      elsif value.instance_of?(String) or value.instance_of?(Pathname)
        "#{s(value.to_s)}"
      else
        raise "invalid type for 'value' argument in JobExecution#convert_value: #{value} is #{value.class}"
      end
    end

    def compile_where_expr(conds_hash)
      conds_hash.map{|k,v| convert_cond(k,v) }.join(' and ')
    end

    def convert_cond(column, cond)
      if cond.nil?
        "#{column} is null"
      elsif cond.instance_of?(Array) # not support subquery
        in_clause = cond.map{|c| convert_cond(column, c)}.join(' or ')
        "(#{in_clause})"
      elsif cond == true or cond == false
        "#{column} is #{cond.to_s}"
      elsif cond.instance_of?(Integer) or cond.instance_of?(Float)
        "#{column} = #{cond}"
      elsif cond.instance_of?(String) or cond.instance_of?(Pathname)
        "#{column} = #{s(cond.to_s)}"
      else
        raise "invalid type for 'cond' argument in JobExecution#convert_cond: #{cond} is #{cond.class}"
      end
    end
  end

end
