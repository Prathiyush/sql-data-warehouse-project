/*
==================================
Bulk insert: Method that loads large amount of data from files(csv/excel), directly into a database.
It does not insert data row by row, but rather the entire data in one go
==================================
*/

--Creating Stored procedure in SQL to LOAD SCRIPTS (Can be run if data gets updated)

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME; -- Monitor ETL Duration to identify bottlenecks, optimize performance, monitor trends and detect issues
	DECLARE @net_start DATETIME, @net_end DATETIME;
	BEGIN TRY
		SET @net_start = GETDATE();
		PRINT '==========================================================';   --Adding print stmts for clarity when sp executes
		PRINT 'LOADING DATA INTO BRONZE LAYER';
		PRINT '==========================================================';
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting data into table: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Prathiyush KG\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,    --skipping first row of csv file (headers)
			FIELDTERMINATOR = ',', --Comma is file delimiter. Values in each row of csv file are comma separated
			TABLOCK --Locks entire table while data being loaded, to improve performance
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD DURATION of TABLE bronze.crm_cust_info = '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------------------';

		PRINT '>> Truncating table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting data into table: bronze.crm_prod_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Prathiyush KG\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '---------------------------------------------------';

		PRINT '>> Truncating table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting data into table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Prathiyush KG\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '---------------------------------------------------';

		TRUNCATE TABLE bronze.erp_cuzt_az12;
		BULK INSERT bronze.erp_cuzt_az12
		FROM 'C:\Users\Prathiyush KG\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '---------------------------------------------------';

		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Prathiyush KG\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '---------------------------------------------------';

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Prathiyush KG\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT '---------------------------------------------------';
		SET @net_end = GETDATE();
		PRINT '>>>> TOTAL DURATION TO LOAD BRONZE LAYER: '+CAST(DATEDIFF(second, @net_start, @net_end) AS NVARCHAR) + ' seconds';
	END TRY
	BEGIN CATCH
		PRINT '====================================';
		PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT '====================================';
	END CATCH

END
