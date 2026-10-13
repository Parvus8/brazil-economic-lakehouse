with national as (
    select
        month,
        product,
        sum(avg_price * samples) / sum(samples) as avg_price,
        sum(samples) as samples
    from {{ ref('fct_fuel_prices_monthly') }}
    group by month, product
),

joined as (
    select
        national.month,
        national.product,
        national.avg_price,
        national.samples,
        macro.usd_brl_avg,
        macro.selic_target_pct,
        macro.ipca_monthly_pct,
        ln(1 + coalesce(macro.ipca_monthly_pct, 0) / 100) as ipca_log_factor
    from national
    inner join {{ ref('int_macro_indicators_monthly') }} as macro
        on national.month = macro.month
),

indexed as (
    select
        *,
        100 * avg_price / first_value(avg_price) over w as fuel_price_index,
        100 * usd_brl_avg / first_value(usd_brl_avg) over w as usd_brl_index,
        100 * exp(sum(ipca_log_factor) over w - first_value(ipca_log_factor) over w) as ipca_index,
        avg_price / lag(avg_price) over w - 1 as fuel_price_mom_pct
    from joined
    window w as (partition by product order by month)
)

select
    month,
    product,
    samples,
    round(avg_price, 3) as avg_price,
    round(avg_price / (ipca_index / 100), 3) as real_avg_price,
    round(fuel_price_mom_pct * 100, 2) as fuel_price_mom_pct,
    round(fuel_price_index, 2) as fuel_price_index,
    round(ipca_index, 2) as ipca_index,
    round(usd_brl_index, 2) as usd_brl_index,
    round(usd_brl_avg, 4) as usd_brl_avg,
    selic_target_pct,
    ipca_monthly_pct
from indexed
