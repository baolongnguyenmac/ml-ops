-- Gold Layer Schema Design

-- Dimension Table: Dim_Date
CREATE TABLE IF NOT EXISTS dim_date (
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

-- -- Dimension Table: Dim_Vendor
-- CREATE TABLE IF NOT EXISTS dim_vendor (
--     vendor_id VARCHAR,
--     vendor_name VARCHAR
-- );

-- -- Dimension Table: Dim_Location
-- CREATE TABLE IF NOT EXISTS dim_location (
--     location_id INTEGER,
--     borough VARCHAR,
--     zone_ VARCHAR,
--     service_zone VARCHAR
-- );

-- -- Dimension Table: Dim_PaymentType
-- CREATE TABLE IF NOT EXISTS dim_payment_type (
--     payment_type VARCHAR,
--     payment_description VARCHAR
-- );

-- -- Dimension Table: Dim_RateCode
-- CREATE TABLE IF NOT EXISTS dim_rate_code (
--     rate_code_id INT,
--     rate_description VARCHAR
-- );


-- -- -- Dimension Table: Dim_Time
-- -- CREATE TABLE IF NOT EXISTS dim_time (
-- --     time_key INTEGER, -- HHMMSS
-- --     hour INTEGER,
-- --     minute INTEGER,
-- --     second INTEGER,
-- --     am_pm VARCHAR
-- -- );

-- Fact Table: Fact_Trips
CREATE TABLE IF NOT EXISTS fact_trips (
    trip_key VARCHAR,
    vendor_id VARCHAR,
    pickup_date_key INTEGER,
    pickup_datetime TIMESTAMPTZ,
    dropoff_date_key INTEGER,
    dropoff_datetime TIMESTAMPTZ,
    payment_type VARCHAR,
    rate_code_id INT,
    pu_location_id INTEGER, -- cần thêm pu & do long/lat vì location_id mới available gần đây
    do_location_id INTEGER,

    -- Metrics
    pickup_longitude        FLOAT,
    pickup_latitude         FLOAT,
    dropoff_longitude       FLOAT,
    dropoff_latitude        FLOAT,
    passenger_count INTEGER,
    trip_distance FLOAT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    surcharge FLOAT,
    improvement_surcharge FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    congestion_surcharge FLOAT,
    airport_fee FLOAT,
    cbd_congestion_fee FLOAT,
    total_amount FLOAT,

    -- Audit fields: 'year', 'month', 'day', 'hour', 'minute', 'source_file', 'source_created_datetime', 'bronze_created_datetime'
    year_ INTEGER,
    month_ INTEGER,
    day_ INTEGER,
    hour_ INTEGER,
    minute_ INTEGER,
    source_file VARCHAR,
    source_created_datetime TIMESTAMPTZ,
    bronze_created_datetime TIMESTAMPTZ,
    silver_created_datetime TIMESTAMPTZ,
    gold_created_datetime TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- select max(full_datetime) from dim_date;
-- select count(*) from dim_date;
