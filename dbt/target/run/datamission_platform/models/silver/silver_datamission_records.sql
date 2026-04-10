
  
    

  create  table "datamission"."silver_silver"."silver_datamission_records__dbt_tmp"
  
  
    as
  
  (
    select distinct *
from "datamission"."silver_staging"."stg_datamission_records"
  );
  