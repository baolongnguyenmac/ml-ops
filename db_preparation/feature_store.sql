-- public
create database feast_offline;
create database feast_online;
create database feast_metadata;

-- feast_offline
select * from feast_driver_hourly_stats

-- feast_metadata
select * from data_sources;
select * from entities;
select * from feature_views;
select * from projects;

-- feast_offline
select * from superstore_feature;
select * from superstore_target;

-- feast_metadata
select * from data_sources;
select * from entities;
select * from feature_views;
