{{ config(severity='warn') }}

select *
from {{ ref('stg_anp__fuel_prices') }}
where sale_price < 0.5
   or sale_price > 50
