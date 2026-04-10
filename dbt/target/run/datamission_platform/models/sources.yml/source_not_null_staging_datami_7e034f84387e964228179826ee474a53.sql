select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select source_record_id
from "datamission"."staging"."datamission_records_raw"
where source_record_id is null



      
    ) dbt_internal_test