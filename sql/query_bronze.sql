select count(*) from nyc_taxi;

select * from nyc_taxi limit 3;

select * from nyc_taxi order by pickup_datetime desc limit 3;

select vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance, count(*) as cnt
from nyc_taxi
--where pickup_datetime > date '2009-04-01' and pickup_datetime < date '2009-04-30'
group by vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance
having count(*) > 1;

select * from nyc_taxi order by pickup_datetime desc limit 3;

show schemas from datalake;

drop table nyc_taxi;

select max(pickup_datetime) from nyc_taxi;

select max(bronze_created_datetime) from nyc_taxi;

select pickup_datetime from nyc_taxi order by pickup_datetime desc limit 9;

select count(*) from nyc_taxi where year(dropoff_datetime)=2005;

select * from nyc_taxi limit 1;

-- drop table nyc_taxi;
-- drop schema bronze;

select max(pickup_datetime) as max_pickup_datetime from nyc_taxi where source_created_datetime <= timestamp '2026-07-31 09:36:13.332929+00';

select max(bronze_created_datetime) from (
    select * from nyc_taxi
    where bronze_created_datetime <= (
        select max(bronze_created_datetime)
        from (
            select * from nyc_taxi
            where bronze_created_datetime > timestamp '2000-01-01 00:00:00.0'
            order by bronze_created_datetime asc
            limit 420000
        )
    ) and bronze_created_datetime > timestamp '2000-01-01 00:00:00.0'
);

select distinct month, year from nyc_taxi order by year, month;
