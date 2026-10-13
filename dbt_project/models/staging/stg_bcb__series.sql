select
    series_name,
    series_code,
    reference_date,
    value
from {{ source('silver', 'bcb_series') }}
