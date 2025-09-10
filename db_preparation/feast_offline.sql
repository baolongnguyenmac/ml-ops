-- Create a new database called 'feast_offline'
-- Connect to the 'master' database to run this snippet
USE master
GO
-- Create the new database if it does not exist already
IF NOT EXISTS (
    SELECT name
        FROM sys.databases
        WHERE name = N'feast_offline'
)
CREATE DATABASE feast_offline
GO

use feast_offline
GO

select * from house_target
go

select * from house_features
go
