with series as (
    select * from {{ ref('stg_bcb__series') }}
),

monthly as (
    select
        date_trunc('month', reference_date)::date as month,
        series_name,
        avg(value) as avg_value,
        arg_max(value, reference_date) as last_value
    from series
    group by 1, 2
)

select
    month,
    max(case when series_name = 'selic_target' then last_value end) as selic_target_pct,
    max(case when series_name = 'ipca_monthly' then last_value end) as ipca_monthly_pct,
    max(case when series_name = 'usd_brl' then avg_value end) as usd_brl_avg
from monthly
group by month
