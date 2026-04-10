select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select record_uid
from "datamission"."silver_staging"."stg_datamission_records"
where record_uid is null



      
    ) dbt_internal_test