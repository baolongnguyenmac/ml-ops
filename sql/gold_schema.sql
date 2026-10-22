-- Gold Layer Schema Design

create schema if not exists staging;

-- Dimension Table: Dim_Date
create table if not exists public.dim_date (
    datetime_key    INT,  -- yyyymmddhh, generated
    full_datetime   TIMESTAMP,
    hour            SMALLINT,
    day             SMALLINT,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    month           SMALLINT,
    month_name      VARCHAR(10),
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN,
    is_rush_hour    BOOLEAN,
    is_holiday      BOOLEAN,
    holiday_name    VARCHAR
);

-- Dimension Table: Dim_Location
create table if not exists public.dim_store_fwd (
    store_and_fwd_key int primary key,
    store_and_fwd_id text unique,
    store_and_fwd_flag text
);

-- Dimension Table: Dim_Store_Fwd
create table if not exists public.dim_location (
    location_key int primary key,
    location_id int unique,
    borough text,
    zone text,
    service_zone text
);

-- Dimension Table: Dim_Vendor
create table if not exists staging.dim_vendor (
    vendor_id text unique,
    vendor_name VARCHAR
);

-- Dimension Table: Dim_PaymentType
create table if not exists staging.dim_payment_type (
    payment_type_id text unique,
    payment_type text
);

-- Dimension Table: Dim_RateCode
create table if not exists staging.dim_rate_code (
    rate_code_id text unique,
    rate_code text
);

-- fact stream
create table if not exists public.fact_trips_stream (
    -- pk
    record_key text unique,

    -- fk
    pu_location_key int,
    do_location_key int,
    payment_type_key text,
    rate_code_key text,
    store_and_fwd_key int,
    vendor_key text,
    pickup_date_key int,
    dropoff_date_key int,

    pickup_longitude float,
    pickup_latitude float,
    dropoff_longitude float,
    dropoff_latitude float,

    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    passenger_count int,
    trip_distance float,
    fare_amount float,
    extra float,
    mta_tax float,
    surcharge float,
    improvement_surcharge float,
    tip_amount float,
    tolls_amount float,
    total_amount float,
    congestion_surcharge float,
    airport_fee float,
    cbd_congestion_fee float,

    -- audit columns
    content_hash text,
    source_file text,
    source_created_datetime timestamp,
    bronze_created_datetime timestamp,
    year int,
    month int,
    silver_created_datetime timestamp,
    silver_updated_datetime timestamp,
    gold_created_datetime timestamp,
    gold_updated_datetime timestamp
);

-- drop table if exists public.fact_trips_stream;

