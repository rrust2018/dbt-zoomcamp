{{ config(materialized="table") }}

with
    with_time_diff as (
        select
            *,
            timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
        from {{ ref("dim_fhv_trips") }}
        where timestamp_diff(dropoff_datetime, pickup_datetime, second) > 0
    ),
    p90_trip_duration as (
        select
            *,
            percentile_cont(trip_duration, 0.90) over (
                partition by year, month, pickup_locationid, dropoff_locationid
            ) as p90
        from with_time_diff
    )
select distinct
    pickup_locationid,
    dropoff_locationid,
    year,
    month,
    p90
from p90_trip_duration