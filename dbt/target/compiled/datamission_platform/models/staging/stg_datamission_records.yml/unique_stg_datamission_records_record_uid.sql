
    
    

select
    record_uid as unique_field,
    count(*) as n_records

from "datamission"."silver_staging"."stg_datamission_records"
where record_uid is not null
group by record_uid
having count(*) > 1


