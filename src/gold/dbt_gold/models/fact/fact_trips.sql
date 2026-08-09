with source as (
    select * from {{ source('silver', 'nyc_taxi') }}
),
dim_payment_type as (
    select * from {{ ref('dim_payment_type') }}
),
dim_rate_code as (
    select * from {{ ref('dim_rate_code') }}
),
dim_vendor as (
    select * from {{ ref('dim_vendor') }}
)

select (
    trip_key,
    vendor_id,
    pickup_date_key,
    pickup_datetime,
    dropoff_date_key,
    dropoff_datetime,
    payment_type,
    rate_code_id,
    pu_location_id,
    do_location_id,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    surcharge,
    improvement_surcharge,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    total_amount,
    year_,
    month_,
    day_,
    hour_,
    minute_,
    source_file,
    source_created_datetime,
    bronze_created_datetime,
    silver_created_datetime
)
from source
    left join 


vendor_id               | integer                     
pickup_datetime         | timestamp(3) with time zone 
dropoff_datetime        | timestamp(3) with time zone 
passenger_count         | integer                     
trip_distance           | double                      
rate_code_id            | integer                     
pickup_longitude        | double                      
pickup_latitude         | double                      
dropoff_longitude       | double                      
dropoff_latitude        | double                      
pu_location_id          | integer                     
do_location_id          | integer                     
fare_amount             | double                      
extra                   | double                      
mta_tax                 | double                      
surcharge               | double                      
improvement_surcharge   | double                      
tip_amount              | double                      
tolls_amount            | double                      
total_amount            | double                      
congestion_surcharge    | double                      
airport_fee             | double                      
cbd_congestion_fee      | double                      
source_file             | varchar                     
source_created_datetime | timestamp(3) with time zone 
bronze_created_datetime | timestamp(3) with time zone 
year                    | integer                     
month                   | integer                     
day                     | integer                     
hour                    | integer                     
minute                  | integer                     
payment_type_id         | varchar                     
store_and_fwd_id        | integer                     
trip_key                | varchar                     
silver_created_datetime | timestamp(3) with time zone 
silver_updated_datetime | timestamp(3) with time zone 