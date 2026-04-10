select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select source_name
from "datamission"."staging"."datamission_records_raw"
where source_name is null



      
    ) dbt_internal_test