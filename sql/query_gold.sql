select * from fact_trips limit 3;

select count(*) from fact_trips;

-- truncate table public.nyc_taxi_staging;
-- truncate table public.fact_trips;

select max(silver_updated_datetime) from nyc_taxi_staging;

select count(*) from nyc_taxi_staging where silver_created_datetime <= timestamp '2026-08-14 13:09:20.531389+07';
select * from nyc_taxi_staging limit 1;

select count(*) from nyc_taxi_staging;
select count(*) from fact_trips_stream;
select
    count(*) as total,
    count(distinct record_key) as num_record_key,
    count(distinct content_hash) as num_content_hash
from fact_trips_stream_tmp;


select * from fact_trips_stream_tmp
where record_key in (
    select record_key
    from fact_trips_stream_tmp
    group by record_key
    having count(*) > 1
)
order by record_key;

select record_key, count(*) as num_record_key
from fact_trips_stream_tmp
group by record_key
having count(*) > 1;

select record_key, content_hash, silver_updated_datetime
from fact_trips_stream_tmp
where record_key='301ebf97ea2614bb2cc5333866293abda67a90f3492e978345853434cd418d3d';

select distinct source_file from fact_trips;

select month, year, count(*) as count_ from fact_trips
group by month, year
order by year, month;

select max(silver_updated_datetime) from fact_trips where year=2009 and month=12;

select count(*) from fact_trips_stream;

select record_key, content_hash from fact_trips_stream
where gold_created_datetime = gold_updated_datetime and silver_created_datetime <> silver_updated_datetime
limit 1;

-- audit stream ------------

select month, year, layer from fact_trips_serving
group by year, month, layer
order by year, month;

select month, year from fact_trips_stream
group by year, month
order by year, month;

select month, year from fact_trips
group by year, month
order by year, month;

select month, year, max(gold_created_datetime) as latest_gold_created from fact_trips_serving
group by year, month
order by year, month;

select month, year from fact_trips_serving
where layer='stream'
group by year, month
order by year, month;

select month, year from fact_trips_serving
where layer='batch'
group by year, month
order by year, month;
