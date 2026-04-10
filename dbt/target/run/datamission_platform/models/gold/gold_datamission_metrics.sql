
  
    

  create  table "datamission"."silver_gold"."gold_datamission_metrics__dbt_tmp"
  
  
    as
  
  (
    with silver_data as (
    select *
    from "datamission"."silver_silver"."silver_datamission_records"
),

metrics as (
    select
        source_name,
        project_id,
        date_trunc('day', loaded_at::timestamp)::date as loaded_day,
        count(*) as records_loaded,
        count(distinct source_record_id) as distinct_source_records,
        count(distinct record_uid) as distinct_record_uids,
        max(loaded_at::timestamp) as last_loaded_at
    from silver_data
    group by 1, 2, 3
)

select *
from metrics
  );
  