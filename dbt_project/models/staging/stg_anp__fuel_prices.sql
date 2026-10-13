select
    date_trunc('month', collected_at)::date as month,
    collected_at,
    region,
    state,
    city,
    reseller,
    reseller_cnpj,
    brand,
    product,
    unit,
    sale_price
from {{ source('silver', 'anp_fuel_prices') }}
