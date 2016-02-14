require 'bricolage/exception'
require 'json'
require 'socket'
require 'forwardable'

module Bricolage

  module StreamingLoad

    class Loader

      def initialize(dwh_proc_id:, task_id:, data_source:, ctl_bucket:,
          table:, work_table: nil, log_table: nil, load_options: nil, sql: nil,
          logger:, noop: false)
        @dwh_proc_id = dwh_proc_id
        @task_id = task_id

        @ds = data_source
        @ctl_bucket = ctl_bucket

        @table = table
        @work_table = work_table
        @log_table = log_table
        @load_options = load_options
        @sql = sql

        @logger = logger
        @noop = noop
        @load_only = load_only

        @start_time = Time.now
        @end_time = nil
      end

      attr_reader :sql

      def work_table
        @work_table || "#{@table}_wk"
      end

      def log_table
        @log_table || "#{@table}_l"
      end

      def log_basic_info
        @logger.info "start_time: #{@start_time}"
        @logger.info "dwh_proc_id: #{@dwh_proc_id}"
      end

      def load
        log_basic_info
        ControlConnection.open(@ds) {|conn|
          task = LoadTask.load(conn, @task_id)
          task.lock {
            unless task.empty?
              load_objects conn, work_table, task
              commit conn, work_table
            end
          }
        }
      end

      def load_objects(conn, work_table, task)
        ManifestFile.create(task, @ctl_bucket, @logger) {|manifest|
          conn.execute "truncate #{work_table}"
          conn.execute(<<-";".strip.gsub(/\s+/, ' '))
            copy #{work_table}
            from '#{manifest.url}'
            credentials '#{manifest.credential_string}'
            manifest
            statupdate false
            #{@load_options}
          ;
          @logger.info "load succeeded: #{manifest.url}" unless @noop
        }
      end

      def commit(conn, work_table, tmp_log_table)
        @end_time = Time.now   # commit_load_log writes this, generate before that
        conn.transaction {
          commit_work_table conn, work_table
          commit_load_log conn, tmp_log_table
        }
      end

      def commit_work_table(conn, work_table)
        insert_stmt = @sql ? @sql.source : "insert into #{@table} select * from #{work_table};"
        conn.execute(insert_stmt)
        # keep work table records for tracing
      end

      def commit_load_log(conn)
        conn.execute(<<-EndSQL)
          insert into #{@loaded_files_table}
          select
              job_process_id
              , start_time
              , #{sql_timestamp @end_time}
              , target_table
              , data_file
          from
              #{tmp_table_name}
          where
              data_file not in (select data_file from #{@staging_files_table})
          ;
        EndSQL
      end

      def sql_timestamp(time)
        %Q(timestamp '#{format_timestamp(time)}')
      end

      def format_timestamp(time)
        time.strftime('%Y-%m-%d %H:%M:%S')
      end

      def sql_string(str)
        escaped = str.gsub("'", "''")
        %Q('#{escaped}')
      end

    end


    class LoadTask

      def initialize(conn)
        @conn = cpnn
      end

      def target_objects
        recs = @conn.execute(<<-";")
          select
              data_file
              , case when l.job_process_id is not null then 'true' else 'false' end as is_loaded
          from
              #{@log_table} l right outer join #{tmp_log_table} t using (data_file)
          ;
        EndSQL
        index = {}
        objects.each do |obj|
          index[obj.url] = obj
        end
        recs.each do |rec|
          obj = index[rec['data_file']]
          obj.loaded = (rec['is_loaded'] == 'true')
        end
        objects.partition(&:loaded)
      end

    end


    class ManifestFile

      def ManifestFile.create(task:, ctl_bucket:, logger:, noop: false)
        manifest = new(task, ctl_bucket, noop: noop)
        logger.info "creating manifest: #{manifest.name}"
        manifest.put
        logger.debug "manifest:\n" + manifest.content if logger.debug?
        yield manifest
        #manifest.delete
      end

      def initialize(task, ctl_bucket, noop: false)
        @task = task
        @ctl_bucket
        @noop = noop
        @url = nil
      end

      def credential_string
        @ctl_bucket.credential_string
      end

      def name
        @name ||= "manifest-#{@task.dwh_proc_id}.json"
      end

      def content
        json = make_manifest_json(task.target_objects)
      end

      def put
        @url = @ctl_bucket.put(@name, content, noop: @noop)
      end

      def delete
        @ctl_bucket.delete(@name, noop: @noop)
      end

      def content
        @content ||= begin
          ents = @task.target_objects.map {|obj|
            { "url" => obj.url, "mandatory" => false }
          }
          obj = { "entries" => ents }
          JSON.pretty_generate(obj)
        end
      end

    end


    class ControlConnection

      def ControlConnection.open(ds, logger)
        ds.open {|conn|
          yield new(ds, conn, logger)
        }
      end

      def initialize(ds, conn, logger)
        @ds = ds
        @conn = conn
        @logger = logger
      end

      def transaction
        execute 'begin transaction'
        yield
        execute 'commit'
      end

      def execute(sql)
        if @noop
          log_query(sql)
        else
          @conn.execute(sql)
        end
      end

      def log_query(sql)
        @logger.info "[#{@ds.name}] #{mask_secrets(sql)}"
      end

      def mask_secrets(log)
        log.gsub(/\bcredentials\s+'.*?'/mi, "credentials '****'")
      end

    end

  end

end
