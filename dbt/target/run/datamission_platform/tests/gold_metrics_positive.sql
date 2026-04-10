select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      select *
from "datamission"."silver_gold"."gold_datamission_metrics"
where records_loaded <= 0
   or distinct_source_records <= 0
   or distinct_record_uids <= 0
      
    ) dbt_internal_test