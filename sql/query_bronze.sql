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

select count(*) from nyc_taxi where year(dropoff_datetime)=2005;

select * from nyc_taxi limit 1;
