drop table if exists dwh_job_results;
drop table if exists dwh_jobs;
drop table if exists dwh_str_load_files;
drop table if exists dwh_str_load_files_incoming;
drop table if exists dwh_str_load_tables;
drop table if exists dwh_tasks;

\i schema/dwh_job_results.ct
\i schema/dwh_jobs.ct
\i schema/dwh_str_load_files.ct
\i schema/dwh_str_load_files_incoming.ct
\i schema/dwh_str_load_tables.ct
\i schema/dwh_tasks.ct

