select distinct *
from {{ ref('stg_datamission_records') }}
