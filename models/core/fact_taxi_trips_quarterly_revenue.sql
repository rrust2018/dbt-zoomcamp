{{
    config(
        materialized='table'
    )
}}

with quarterly_revenue as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        extract(year from pickup_datetime) || "/Q" || extract(quarter from pickup_datetime) as year_quarter,
        sum(total_amount) as total_amount
    from {{ ref("fact_trips") }}
    where extract(year from pickup_datetime) in(2019, 2020)
    group by 1, 2, 3, 4
)
select
    curr.year,
    curr.quarter,
    curr.service_type,
    curr.total_amount as curr_total,
    prev.total_amount AS prev_total,
    (curr.total_amount/prev.total_amount - 1) * 100 as yoy_growth
from quarterly_revenue as curr
    left join quarterly_revenue as prev
    on curr.service_type = prev.service_type
    and curr.year = (prev.year + 1)
    and curr.quarter = prev.quarter