select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select project_id
from "datamission"."silver_gold"."gold_datamission_metrics"
where project_id is null



      
    ) dbt_internal_test