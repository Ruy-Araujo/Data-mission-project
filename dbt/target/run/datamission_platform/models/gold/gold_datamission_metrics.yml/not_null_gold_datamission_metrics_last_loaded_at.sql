select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select last_loaded_at
from "datamission"."silver_gold"."gold_datamission_metrics"
where last_loaded_at is null



      
    ) dbt_internal_test