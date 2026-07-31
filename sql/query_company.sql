select max(pickup_datetime) from nyc_taxi;

select created_datetime, count(*) from nyc_taxi
group by created_datetime;

select count(*) from public.nyc_taxi where created_datetime > timestamp '2026-06-30 05:45:49.777569';

select count(*) from public.nyc_taxi;

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
