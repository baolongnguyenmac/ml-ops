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

select * from nyc_taxi;

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

select month, year, count(*) from nyc_taxi
group by month, year
order by year, month;
