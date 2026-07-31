-- Gold Layer Schema Design

-- Dimension Table: Dim_Vendor
CREATE TABLE IF NOT EXISTS IF NOT EXISTS dim_vendor (
    vendor_id VARCHAR PRIMARY KEY,
    vendor_name VARCHAR -- To be populated via mapping or lookup
);

-- Dimension Table: Dim_Location
CREATE TABLE IF NOT EXISTS IF NOT EXISTS dim_location (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR,
    community_area VARCHAR,
    zone VARCHAR,
    longitude DOUBLE,
    latitude DOUBLE
);

-- Dimension Table: Dim_PaymentType
CREATE TABLE IF NOT EXISTS IF NOT EXISTS dim_payment_type (
    payment_type VARCHAR PRIMARY KEY,
    payment_description VARCHAR
);

-- Dimension Table: Dim_RateCode
CREATE TABLE IF NOT EXISTS IF NOT EXISTS dim_rate_code (
    rate_code_id DOUBLE PRIMARY KEY,
    rate_description VARCHAR
);

-- Dimension Table: Dim_Date
CREATE TABLE IF NOT EXISTS IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY, -- YYYYMMDD
    full_date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    day_of_week VARCHAR,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR
);

-- -- Dimension Table: Dim_Time
-- CREATE TABLE IF NOT EXISTS IF NOT EXISTS dim_time (
--     time_key INTEGER PRIMARY KEY, -- HHMMSS
--     hour INTEGER,
--     minute INTEGER,
--     second INTEGER,
--     am_pm VARCHAR
-- );

-- Fact Table: Fact_Trips
CREATE TABLE IF NOT EXISTS IF NOT EXISTS fact_trips (
    trip_key VARCHAR PRIMARY KEY,
    vendor_id VARCHAR REFERENCES dim_vendor(vendor_id),
    pickup_date_key INTEGER REFERENCES dim_date(date_key),
    pickup_datetime TIMESTAMPTZ,
    dropoff_date_key INTEGER REFERENCES dim_date(date_key),
    dropoff_datetime TIMESTAMPTZ,
    pu_location_id INTEGER REFERENCES dim_location(location_id),
    do_location_id INTEGER REFERENCES dim_location(location_id),
    payment_type VARCHAR REFERENCES dim_payment_type(payment_type),
    rate_code_id DOUBLE REFERENCES dim_rate_code(rate_code_id),

    -- Metrics
    passenger_count INTEGER,
    trip_distance DOUBLE,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    surcharge DOUBLE,
    improvement_surcharge DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    cbd_congestion_fee DOUBLE,
    total_amount DOUBLE,

    -- Metadata
    source_file VARCHAR,
    gold_created_datetime TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
