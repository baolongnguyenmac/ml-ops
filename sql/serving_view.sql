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
);
