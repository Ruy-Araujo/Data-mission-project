select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select distinct_record_uids
from "datamission"."silver_gold"."gold_datamission_metrics"
where distinct_record_uids is null



      
    ) dbt_internal_test