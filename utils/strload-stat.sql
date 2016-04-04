select
    dwh_task_seq
    , dwh_task_id
    , utc_submit_time
    , utc_start_time
    , dwh_job_seq
    , status
    , substring(message, 1, 30) as err_msg
from
    dwh_tasks t
    left outer join dwh_jobs j using (dwh_task_seq)
    left outer join dwh_job_results r using (dwh_job_seq)
order by
    dwh_task_seq
    , dwh_job_seq
;
