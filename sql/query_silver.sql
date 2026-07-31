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

describe nyc_taxi;
