{{
    config(
        materialized = 'incremental',
        unique_key = 'record_key',
        cluster_by = ['year', 'month'],
        incremental_strategy = 'merge'
    )
}}
-- cluster_by will affect how the data is stored on disk, and indexed to limit scans
-- `incremental_predicates` accepts a list of SQL statements.
--`DBT_INTERNAL_DEST` and `DBT_INTERNAL_SOURCE` are the standard aliases for the target table and temporary table, respectively, during an incremental run using the merge strategy.

-- tôi thử add phần này vào config thì bị lỗi, không rõ nguyên nhân
-- incremental_predicates = [
--     '(DBT_INTERNAL_DEST.year, DBT_INTERNAL_DEST.month) IN (SELECT DISTINCT year, month FROM DBT_INTERNAL_SOURCE)'
-- ]

with source as (
    select * from {{ source('staging', 'nyc_taxi_staging') }}
    {% if is_incremental() %}
    -- where year >= (select max(year)  from {{ this }})
    --     and month >= (select max(month) from {{ this }})
    {% endif %}
),
dim_location as (
    select * from {{ ref('dim_location') }}
),
dim_payment_type as (
    select * from {{ ref('dim_payment_type') }}
),
dim_rate_code as (
    select * from {{ ref('dim_rate_code') }}
),
dim_store_fwd as (
    select * from {{ ref('dim_store_fwd') }}
),
dim_vendor as (
    select * from {{ ref('dim_vendor') }}
)

select
    -- PK
    s.record_key,

    -- FK
    pu_dl.surrogate_id as surrogate_pu_location_id,
    do_dl.surrogate_id as surrogate_do_location_id,
    dp.surrogate_id as surrogate_payment_id,
    dr.surrogate_id as surrogate_rate_code_id,
    dsf.surrogate_id as surrogate_storage_fwd_id,
    dv.surrogate_id as surrogate_vendor_id,
    cast(to_char(date_trunc('hour', s.pickup_datetime),  'yyyymmddhh24') as int) as pickup_date_key,
    cast(to_char(date_trunc('hour', s.dropoff_datetime), 'yyyymmddhh24') as int) as dropoff_date_key,

    -- since there's no info of location id at stated
    s.pickup_longitude,
    s.pickup_latitude,
    s.dropoff_longitude,
    s.dropoff_latitude,

    s.pickup_datetime,
    s.dropoff_datetime,
    s.passenger_count,
    s.trip_distance,
    s.fare_amount,
    s.extra,
    s.mta_tax,
    s.surcharge,
    s.improvement_surcharge,
    s.tip_amount,
    s.tolls_amount,
    s.total_amount,
    s.congestion_surcharge,
    s.airport_fee,
    s.cbd_congestion_fee,

    -- audit fields
    s.content_hash,
    s.source_file,
    s.source_created_datetime,
    s.bronze_created_datetime,
    s.year,
    s.month,
    s.silver_created_datetime,
    s.silver_updated_datetime,
    current_timestamp as gold_created_datetime

from source as s
    left join dim_location as pu_dl on s.pu_location_id=pu_dl.business_location_id
    left join dim_location as do_dl on s.do_location_id=do_dl.business_location_id
    left join dim_payment_type as dp on s.payment_type=dp.payment_type
    left join dim_rate_code as dr on s.rate_code=dr.rate_code
    left join dim_store_fwd as dsf on s.store_and_fwd_flag=dsf.store_and_fwd_flag
    left join dim_vendor as dv on s.vendor=dv.vendor_name

{% if is_incremental() %}
-- where s.silver_updated_datetime > (select max(silver_updated_datetime) from {{ this }})
{% endif %}
