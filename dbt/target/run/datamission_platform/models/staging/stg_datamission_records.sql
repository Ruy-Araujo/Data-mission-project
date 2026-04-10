
  create view "datamission"."silver_staging"."stg_datamission_records__dbt_tmp"
    
    
  as (
    select
	coalesce(cast(source_record_id as text), cast(record_uid as text)) as source_record_id,
	cast(record_uid as text) as record_uid,
	cast(source_name as text) as source_name,
	cast(project_id as text) as project_id,
	cast(loaded_at as timestamp) as loaded_at,
	cast(ingestion_run_id as text) as ingestion_run_id
from "datamission"."staging"."datamission_records_raw"
  );