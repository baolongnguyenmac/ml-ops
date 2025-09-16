-- public
create database feast_offline;
create database feast_online;
create database feast_metadata;

-- feast_offline
select * from feast_driver_hourly_stats order by event_timestamp desc limit 100

select * from feast_driver_hourly_stats
where driver_id='1001'
order by feast_driver_hourly_stats desc

-- feast_metadata
select * from data_sources;
select * from entities;
select * from feature_views;
select * from projects;
select * from feature_services

-- feast_offline
select * from superstore_feature order by "Order Date" limit 10;
select "Order Date", "Order ID", "Profit" from superstore_target order by "Order Date" desc limit 100;

-- feast_metadata
select * from data_sources;
select * from entities;
select * from feature_views;
