select
    month,
    region,
    state,
    product,
    unit,
    count(*) as samples,
    count(distinct reseller_cnpj) as stations,
    round(avg(sale_price), 3) as avg_price,
    round(median(sale_price), 3) as median_price,
    min(sale_price) as min_price,
    max(sale_price) as max_price
from {{ ref('stg_anp__fuel_prices') }}
group by month, region, state, product, unit
