CREATE OR REPLACE VIEW fact_trips_serving AS

SELECT 
    t.*,
    'batch' as layer
FROM fact_trips t

UNION ALL

SELECT 
    s.*,
    'stream' as layer
FROM fact_trips_stream s
WHERE s.gold_created_datetime > (
    SELECT COALESCE(MAX(gold_created_datetime), '2000-01-01')
    FROM fact_trips
) or (
    s.month > (
        select max(month) from fact_trips
    ) and
    s.year >= (
        select max(year) from fact_trips
    )
);

-- audit

select distinct(month, year) from fact_trips
where gold_created_datetime = (SELECT COALESCE(MAX(gold_created_datetime), '2000-01-01')
    FROM fact_trips);

select max(gold_created_datetime)
from fact_trips_stream
where month=5 and year=2009;

select month, year, layer, count(*) from fact_trips_serving
group by year, month, layer
order by year, month;

select month, year, count(*) from fact_trips
group by year, month
order by year, month;

select month, year, count(*) from fact_trips_stream
group by year, month
order by year, month;
