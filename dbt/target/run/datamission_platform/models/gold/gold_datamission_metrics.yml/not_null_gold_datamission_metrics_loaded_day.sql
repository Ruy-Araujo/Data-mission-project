select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select loaded_day
from "datamission"."silver_gold"."gold_datamission_metrics"
where loaded_day is null



      
    ) dbt_internal_test