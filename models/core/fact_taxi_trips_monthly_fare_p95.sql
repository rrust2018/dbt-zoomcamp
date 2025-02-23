{{ config(materialized="table") }}

with
    filtered_trips as (
        select
            service_type,
            extract(year from pickup_datetime) as year,
            extract(month from pickup_datetime) as month,
            fare_amount
        from {{ ref("fact_trips") }}
        where
            fare_amount > 0
            and trip_distance > 0
            and payment_type_description in ("Cash", "Credit card")
    ),
    percentile as (
        select
            service_type,
            year,
            month,
            percentile_cont(fare_amount, 0.97) over (
                partition by service_type, year, month
            ) as fare_p97,
            percentile_cont(fare_amount, 0.95) over (
                partition by service_type, year, month
            ) as fare_p95,
            percentile_cont(fare_amount, 0.90) over (
                partition by service_type, year, month
            ) as fare_p90
        from filtered_trips
    )
select distinct service_type, year, month, fare_p97, fare_p95, fare_p90
from percentile
where year = 2020 and month = 4
order by service_type
