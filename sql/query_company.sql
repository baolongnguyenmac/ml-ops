select distinct source_file from nyc_taxi order by source_file asc;

select max(source_file) from nyc_taxi;

select max(pickup_datetime), min(pickup_datetime) from nyc_taxi;

select created_datetime, count(*) from nyc_taxi
group by created_datetime;

select count(*) from public.nyc_taxi where created_datetime > timestamp '2026-06-30 05:45:49.777569';

select count(*) from nyc_taxi;

select count(*) from nyc_taxi where extract(year from pickup_datetime)=2025 and extract(month from pickup_datetime)=12;

select count(*) from nyc_taxi
where source_created_datetime <= (
	select max(source_created_datetime)
	from (
		select * from nyc_taxi
		where source_created_datetime > timestamp '2026-06-30 12:45:49.777569+00'
		order by pickup_datetime asc
		limit 420000
	)
) and source_created_datetime > timestamp '2026-06-30 12:45:49.777569+00';

select count(*) from nyc_taxi
where source_created_datetime <= timestamp '2026-06-30 12:49:52.616';

select max(source_created_datetime)
from (
	select * from nyc_taxi
	where source_created_datetime > timestamp '2000-01-01 00:00:00.0'
	order by pickup_datetime asc
	limit 420000
);

select * from nyc_taxi
where source_created_datetime > timestamp '2000-01-01 00:00:00.0'
limit 420000;

select max(source_created_datetime) from nyc_taxi;

select count(*) from public.nyc_taxi where source_created_datetime > timestamp '2026-06-30 12:45:49.777569';

select * from nyc_taxi order by source_created_datetime desc limit 1;

select count(*) from nyc_taxi;

select source_created_datetime, count(*) from nyc_taxi
group by source_created_datetime;

select count(*) from public.nyc_taxi
where
	extract(month from pickup_datetime)=1
	and extract(year from pickup_datetime)=2013;

select * from public.nyc_taxi where pickup_datetime > '2019-03-31 23:59:59' and pickup_datetime < '2019-06-30 23:59:59';

select * from public.nyc_taxi order by pickup_datetime desc limit 1;

select count(*) from public.nyc_taxi where
	extract(month from pickup_datetime)=5
	and extract(year from pickup_datetime)=2019;

select pickup_datetime from public.nyc_taxi order by pickup_datetime desc limit 1;

select count(*) from public.nyc_taxi where pickup_datetime > '2009-01-01' and pickup_datetime < '2009-02-01';

select * from public.nyc_taxi order by pickup_datetime desc limit 3;

select * from public.nyc_taxi where (pickup_datetime < '2009-07-27 19:16:00.000') and (pickup_datetime > '2009-06-27 19:16:00.000');

select * from public.nyc_taxi where payment_type = '2' limit 1;

ALTER TABLE nyc_taxi 
RENAME COLUMN source_created_datetime TO source_created_datetime;

select * from nyc_taxi limit 1;

drop table nyc_taxi;

select count(*) from nyc_taxi;

select min(pickup_datetime), max(pickup_datetime) from nyc_taxi
where source_created_datetime > timestamp '2026-07-21 14:56:00.616048+00' and source_created_datetime <= timestamp '2026-07-21 14:57:40.827332';

select min(source_created_datetime)
from nyc_taxi
where date(pickup_datetime) = '2009-04-01';

select source_created_datetime
from nyc_taxi
where pickup_datetime = (
	select min(pickup_datetime)
	from nyc_taxi
	where source_created_datetime > timestamp '2026-07-21 14:56:00.616048+00' and source_created_datetime <= timestamp '2026-07-21 14:57:40.827332'
);

select * from nyc_taxi
where
	extract(month from pickup_datetime)<>2 and
	extract(year from pickup_datetime)=2002;

-- drop database bronze_metastore;
-- drop database silver_metastore;
-- create database metastore;

-- drop table nyc_taxi;

select * from nyc_taxi limit 1;

select max(source_created_datetime) from (
	select * from nyc_taxi
	where source_created_datetime <= (
		select max(source_created_datetime)
		from (
			select * from nyc_taxi
			where source_created_datetime > timestamp '2026-08-12 18:29:35.957459'
			order by source_created_datetime asc
			limit 420000
		)
	) and source_created_datetime > timestamp '2026-08-12 18:29:35.957459'
);

select max(source_file) from nyc_taxi;

select source_created_datetime from nyc_taxi_stream;

select * from nyc_taxi_stream
where
	vendor_id='1'
	and pickup_datetime=timestamp '2024-10-10 21:36:44.000'
	and dropoff_datetime=timestamp '2024-10-10 21:45:03.000'
	and passenger_count=1
	and trip_distance=1.8
	and rate_code_id=1
	and store_and_fwd_flag='N'
	and pu_location_id='170'
	and do_location_id='141'
	and payment_type='1'
	and fare_amount=10.7
	and extra=3.5
	and mta_tax=0.5
	and improvement_surcharge=1
	and tip_amount=3.1
	and total_amount=18.8
	and congestion_surcharge=2.5
	and source_created_datetime=timestamp '2026-09-30 02:43:00.293';

select max(source_created_datetime) from nyc_taxi_stream;
