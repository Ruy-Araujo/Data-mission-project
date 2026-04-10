select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select records_loaded
from "datamission"."silver_gold"."gold_datamission_metrics"
where records_loaded is null



      
    ) dbt_internal_test