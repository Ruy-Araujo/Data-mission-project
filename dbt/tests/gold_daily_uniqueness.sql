select
    project_id,
    loaded_day,
    count(*) as row_count
from {{ ref('gold_datamission_metrics') }}
group by 1, 2
having count(*) > 1
