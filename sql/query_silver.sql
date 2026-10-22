select max(bronze_created_datetime) from nyc_taxi where bronze_created_datetime > TIMESTAMP '2026-07-22 08:11:36.095000';
-- 2026-07-22 08:19:35.065 UTC

select max(pickup_datetime) from nyc_taxi;

select * from nyc_taxi limit 1;

-- bronze_created_datetime 2026-07-22 08:11:36.095000
select min(pickup_datetime) from nyc_taxi where bronze_created_datetime > timestamp '2026-07-22 08:11:36.095000';

select min(pickup_datetime) from nyc_taxi where silver_created_datetime <> silver_updated_datetime;

select trip_key, count_ from (
    select trip_key, count(*) as count_
    from nyc_taxi
    where year(pickup_datetime)=2010
    group by trip_key
)
where count_ > 1;

select * from nyc_taxi limit 10;

select max(pickup_datetime) from nyc_taxi where bronze_created_datetime < timestamp '2026-07-24 09:55:43.899000';

select count(*) from nyc_taxi where year(pickup_datetime)=2009;

select * from "nyc_taxi$history"
order by version desc
limit 10;

-- drop table nyc_taxi;
-- drop schema silver;

select * from nyc_taxi limit 1;

select max(pickup_datetime) from nyc_taxi where source_created_datetime <= timestamp '2026-07-31 09:34:50.542609+00';

describe nyc_taxi;

select count(*) from nyc_taxi
where vendor_id='DDS';

select * from nyc_taxi where date_trunc('minute', silver_created_datetime) = timestamp '2026-08-14 06:34:00';

select count(*) from nyc_taxi
where silver_updated_datetime <= (
    select max(silver_updated_datetime)
    from (
        select * from nyc_taxi
        where silver_updated_datetime > timestamp '2026-08-14 06:09:20.531389'
        order by silver_updated_datetime asc
        limit 420000
    )
) and silver_updated_datetime > timestamp '2026-08-14 06:09:20.531389';

select max(silver_updated_datetime) from (
    select * from nyc_taxi
    where silver_updated_datetime <= (
        select max(silver_updated_datetime)
        from (
            select * from nyc_taxi
            where silver_updated_datetime > timestamp '2000-01-01 00:00:00.0'
            order by silver_updated_datetime asc
            limit 420000
        )
    ) and silver_updated_datetime > timestamp '2000-01-01 00:00:00.0'
);

select distinct month, year from nyc_taxi order by year, month;

select month, year, count(*) from (
    select * from nyc_taxi
    where silver_updated_datetime <= (
        select max(silver_updated_datetime)
        from (
            select * from nyc_taxi
            where silver_updated_datetime > timestamp '2026-08-20 08:04:09.963899'
            order by silver_updated_datetime asc
            limit 420000
        )
    ) and silver_updated_datetime > timestamp '2026-08-20 08:04:09.963899'
)
group by month, year
order by year, month;


select max(silver_updated_datetime) from nyc_taxi;

select count(*) from nyc_taxi;

select month, year, count(*) as count_ from nyc_taxi
group by month, year
order by year, month;

select count(*) from silver.nyc_taxi_stream;

select count(*) from bronze.nyc_taxi_stream;

select * from silver.nyc_taxi_stream limit 1;

select
    avg(time_to_bronze) as mean_time_to_bronze,
    stddev(time_to_bronze) as std_time_to_bronze,
    avg(time_to_silver) as mean_time_to_silver,
    stddev(time_to_silver) as std_time_to_silver
from (
    select
        source_created_datetime,
        bronze_created_datetime,
        silver_created_datetime,
        date_diff('second', source_created_datetime, bronze_created_datetime) as time_to_bronze,
        date_diff('second', source_created_datetime, silver_created_datetime) as time_to_silver
    from silver.nyc_taxi_stream
    where month=10 and year=2024
);

select
    source_created_datetime,
    bronze_created_datetime,
    silver_created_datetime,
    date_diff('second', source_created_datetime, bronze_created_datetime) as time_to_bronze,
    date_diff('second', source_created_datetime, silver_created_datetime) as time_to_silver
from lakehouse.silver.nyc_taxi_stream;
-- where
--     month=10 and year=2021
--     and record_key='7278d1005e222b60a1805d9f4c5b6061c9efe537a87ab26b728b31c5c2de7174';

select * from bronze.nyc_taxi limit 1;

select
    -- record_key,
    -- source_created_datetime,
    -- bronze_created_datetime
    -- *
    date_diff('second', source_created_datetime, bronze_created_datetime)
from lakehouse.bronze.nyc_taxi_stream
where record_key='7278d1005e222b60a1805d9f4c5b6061c9efe537a87ab26b728b31c5c2de7174';

select distinct rate_code, rate_code_id from nyc_taxi;

select
    count(*) as total,
    count(distinct record_key) as num_record_key,
    count(distinct content_hash) as num_content_hash
from nyc_taxi_stream;

select * from nyc_taxi_stream;

select record_key, content_hash from nyc_taxi_stream
where record_key='cceb81c0077a1997670a5eb0795b00f78967b1a07ce13b957e11c6be7f948d4c';

select max(silver_updated_datetime) from nyc_taxi;

select count(*) from nyc_taxi_stream
where day(silver_created_datetime)=13;

select month, year, count(*) from nyc_taxi_stream
group by year, month
order by year, month;
