module Bricolage
  class LogFilePath
    def LogFilePath.default
      if dir = ENV['BRICOLAGE_LOG_DIR']
        new("#{dir}/%{std}.log")
      elsif path = ENV['BRICOLAGE_LOG_PATH']
        new(path)
      else
        nil
      end
    end

    def initialize(template)
      @template = template
    end

    Params = Struct.new(:job_ref, :jobnet_id, :job_start_time, :jobnet_start_time)

    def format(job_ref:, jobnet_id:, job_start_time:, jobnet_start_time:)
      return nil unless @template
      params = Params.new(job_ref, jobnet_id, job_start_time, jobnet_start_time)
      fill_template(@template, params)
    end

    def fill_template(template, params)
      template.gsub(/%\{\w+\}/) {|var|
        case var
        when '%{std}' then standard_format(params)
        when '%{jobnet_start_date}' then jobnet_start_date(params)
        when '%{jobnet_start_time}' then jobnet_start_time(params)
        when '%{job_start_date}' then job_start_date(params)
        when '%{job_start_time}' then job_start_time(params)
        when '%{jobnet}', '%{net}', '%{jobnet_id}', '%{net_id}', '%{flow}', '%{flow_id}' then jobnet_id(params)
        when '%{subsystem}' then subsystem_name(params)
        when '%{job}', '%{job_id}' then job_name(params)
        else
          raise ParameterError, "bad log path variable: #{var}"
        end
      }
    end

    STD_TEMPLATE = '%{jobnet_start_date}/%{jobnet}/%{jobnet_start_time}/%{subsystem}-%{job}'

    def standard_format(params)
      fill_template(STD_TEMPLATE, params)
    end

    def jobnet_start_date(params)
      params.jobnet_start_time.strftime('%Y%m%d')
    end

    def jobnet_start_time(params)
      params.jobnet_start_time.strftime('%Y%m%d_%H%M%S%L')
    end

    def job_start_date(params)
      params.start_time.strftime('%Y%m%d')
    end

    def job_start_time(params)
      params.start_time.strftime('%Y%m%d_%H%M%S%L')
    end

    def jobnet_id(params)
      params.jobnet_id.gsub('/', '::')
    end

    def subsystem_name(params)
      params.job_ref.subsystem
    end

    def job_name(params)
      params.job_ref.name
    end
  end
end
