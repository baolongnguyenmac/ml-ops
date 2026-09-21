select * from fact_trips limit 3;

select count(*) from fact_trips;

-- truncate table public.nyc_taxi_staging;
-- truncate table public.fact_trips;

select max(silver_updated_datetime) from nyc_taxi_staging;

select count(*) from nyc_taxi_staging where silver_created_datetime <= timestamp '2026-08-14 13:09:20.531389+07';
select * from nyc_taxi_staging limit 1;

select count(*) from nyc_taxi_staging;
select count(*) from fact_trips;

select distinct source_file from fact_trips;

select month, year, count(*) as count_ from fact_trips
group by month, year
order by year, month;

select max(silver_updated_datetime) from fact_trips where year=2009 and month=12;
