/*
=======================================
CREATING DATABASE AND SCHEMAS
=======================================
*/
-- Creating DWH Database
USE master;
CREATE DATABASE Datawarehouse;
USE Datawarehouse;

-- Creating Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
