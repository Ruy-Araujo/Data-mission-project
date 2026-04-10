select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      select
    project_id,
    loaded_day,
    count(*) as row_count
from "datamission"."silver_gold"."gold_datamission_metrics"
group by 1, 2
having count(*) > 1
      
    ) dbt_internal_test