/*
Create Database Schema

Script Purpose:
  This script creates a new database named 'Datawarehouse' after checking if it already exists.
  If the database exists, it is dropped and recreated. Additionally, the scripts set up three schemas within the database: 'bronze', 'silver', 'gold'.

Warning:
  Running this script will drop the entire 'Datawarehouse' database if it exists.
  All the data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script
*/
USE master;
GO
Create Database DataWarehouse;
USE DataWarehouse;

-- Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

CREATE Schema bronze;
GO
CREATE Schema silver;
GO
CREATE Schema gold;
GO


