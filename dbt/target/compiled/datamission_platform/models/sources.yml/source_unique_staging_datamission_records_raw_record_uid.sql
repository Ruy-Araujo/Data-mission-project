
    
    

select
    record_uid as unique_field,
    count(*) as n_records

from "datamission"."staging"."datamission_records_raw"
where record_uid is not null
group by record_uid
having count(*) > 1


