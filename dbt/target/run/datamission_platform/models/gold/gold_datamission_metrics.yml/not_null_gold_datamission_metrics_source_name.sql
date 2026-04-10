select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select source_name
from "datamission"."silver_gold"."gold_datamission_metrics"
where source_name is null



      
    ) dbt_internal_test