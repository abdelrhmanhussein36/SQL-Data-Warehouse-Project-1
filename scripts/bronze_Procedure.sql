/*
======================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
======================================================
Script Purpose:

	This stored procedure loads data into the 'bronze' schema from external CSV files.

It performs the following actions:

	* Drops the bronze tables before loading data.
	* Uses the BULK INSERT command to load data from csv Files to bronze tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC bronze.load_bronze;

===================================
DDL Script: Create Bronze Tables
===================================
Script Purpose:

	This script creates tables in the 'bronze' schema, dropping existing tables if they already exist.
	Run this script to re-define the DDL structure of 'bronze' Tables
==================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	DECLARE @total_start_time DATETIME, @total_end_time DATETIME;
	DECLARE @duration INT;
	
	BEGIN TRY
		PRINT '========================';
		PRINT 'Loading The Bronze Layer';
		PRINT '========================';
		
		SET @total_start_time = GETDATE();
		
		SET @start_time = GETDATE();
		PRINT '1. Dropping and creating CRM Customer Info table...';
		IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL
			DROP TABLE bronze.crm_cust_info;

		CREATE TABLE bronze.crm_cust_info (
			cst_id INT,
			cst_key NVARCHAR(50),
			cst_firstname NVARCHAR(50),
			cst_lastname NVARCHAR(50),
			cst_marital_status NVARCHAR(50),
			cst_gndr NVARCHAR(50),
			cst_create_date DATE
		);
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Study\Udemy - The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero 2025-6\25. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		SET @duration = DATEDIFF(SECOND, @start_time, @end_time);
		PRINT '   ✓ CRM Customer Info loaded successfully.';
		PRINT '   >> Load Duration: '+ CAST(@duration AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------';

		PRINT '';
		SET @start_time = GETDATE();
		PRINT '2. Dropping and creating CRM Product Info table...';
		IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL
			DROP TABLE bronze.crm_prd_info;

		CREATE TABLE bronze.crm_prd_info (
			prd_id INT,
			prd_key NVARCHAR(50),
			prd_nm NVARCHAR(50),
			prd_cost DECIMAL(10, 2),
			prd_line NVARCHAR(50),
			prd_start_dt DATETIME,
			prd_end_dt DATETIME
		);
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Study\Udemy - The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero 2025-6\25. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		SET @duration = DATEDIFF(SECOND, @start_time, @end_time);
		PRINT '   ✓ CRM Product Info loaded successfully.';
		PRINT '   >> Load Duration: '+ CAST(@duration AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------';

		PRINT '';
		SET @start_time = GETDATE();
		PRINT '3. Dropping and creating CRM Sales Details table...';
		IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL
			DROP TABLE bronze.crm_sales_details;

		CREATE TABLE bronze.crm_sales_details (
			sls_ord_num NVARCHAR(50),
			sls_prd_key NVARCHAR(50),
			sls_cust_id INT,
			sls_order_dt INT,
			sls_ship_dt INT,
			sls_due_dt INT,
			sls_sales INT,
			sls_quantity INT,
			sls_price INT
		);
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Study\Udemy - The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero 2025-6\25. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		SET @duration = DATEDIFF(SECOND, @start_time, @end_time);
		PRINT '   ✓ CRM Sales Details loaded successfully.';
		PRINT '   >> Load Duration: '+ CAST(@duration AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------';

		PRINT '';
		SET @start_time = GETDATE();
		PRINT '4. Dropping and creating ERP Customer AZ12 table...';
		IF OBJECT_ID ('bronze.erp_CUST_AZ12','U') IS NOT NULL
			DROP TABLE bronze.erp_CUST_AZ12;

		CREATE TABLE bronze.erp_CUST_AZ12 (
			CID NVARCHAR(50),
			BDATE DATE,
			GEN NVARCHAR(50)
		);
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'D:\Study\Udemy - The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero 2025-6\25. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		SET @duration = DATEDIFF(SECOND, @start_time, @end_time);
		PRINT '   ✓ ERP Customer AZ12 loaded successfully.';
		PRINT '   >> Load Duration: '+ CAST(@duration AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------';

		PRINT '';
		SET @start_time = GETDATE();
		PRINT '5. Dropping and creating ERP Location A101 table...';
		IF OBJECT_ID ('bronze.erp_LOC_A101','U') IS NOT NULL
			DROP TABLE bronze.erp_LOC_A101;

		CREATE TABLE bronze.erp_LOC_A101 (
			CID NVARCHAR(50),
			CNTRY NVARCHAR(50)
		);
		BULK INSERT bronze.erp_LOC_A101
		FROM 'D:\Study\Udemy - The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero 2025-6\25. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		SET @duration = DATEDIFF(SECOND, @start_time, @end_time);
		PRINT '   ✓ ERP Location A101 loaded successfully.';
		PRINT '   >> Load Duration: '+ CAST(@duration AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------';

		PRINT '';
		SET @start_time = GETDATE();
		PRINT '6. Dropping and creating ERP Product Category G1V2 table...';
		IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2','U') IS NOT NULL
			DROP TABLE bronze.erp_PX_CAT_G1V2;

		CREATE TABLE bronze.erp_PX_CAT_G1V2 (
			ID NVARCHAR(50),
			CAT NVARCHAR(50),
			SUBCAT NVARCHAR(50),
			MAINTENANCE  NVARCHAR(50)
		);
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'D:\Study\Udemy - The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero 2025-6\25. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FIRSTROW =2,
		FIELDTERMINATOR =',',
		TABLOCK
		)
		SET @end_time = GETDATE();
		SET @duration = DATEDIFF(SECOND, @start_time, @end_time);
		PRINT '   ✓ ERP Product Category G1V2 loaded successfully.';
		PRINT '   >> Load Duration: '+ CAST(@duration AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------';

		PRINT '';
		SET @total_end_time = GETDATE();
		SET @duration = DATEDIFF(SECOND, @total_start_time, @total_end_time);
		PRINT '=================================';
		PRINT 'Bronze Layer Loading Complete!';
		PRINT '6 tables successfully loaded.';
		PRINT '>>> TOTAL DURATION: '+ CAST(@duration AS NVARCHAR) + ' Seconds <<<';
		PRINT '=================================';
	END TRY
	BEGIN CATCH
		PRINT '';
		PRINT '❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌';
		PRINT 'ERROR: Bronze Layer Loading Failed!';
		PRINT '❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌';
		PRINT '';
		
		PRINT 'Error Details:';
		PRINT '--------------';
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR(10));
		
		PRINT '';
		PRINT 'Troubleshooting Steps:';
		PRINT '----------------------';
		PRINT '1. Check if the CSV file exists at the specified path';
		PRINT '2. Verify the CSV file is not open in another program';
		PRINT '3. Ensure the CSV format matches the table structure';
		PRINT '4. Check for data type mismatches in the CSV file';
		PRINT '5. Verify you have proper permissions to access the file';
		PRINT '6. Ensure the bronze schema exists in the database';
		
		PRINT '';
		PRINT 'File Path Check:';
		PRINT '----------------';
		DECLARE @ErrorMessage NVARCHAR(4000);
		SET @ErrorMessage = ERROR_MESSAGE();
		
		IF @ErrorMessage LIKE '%Cannot open file%' 
		   OR @ErrorMessage LIKE '%file not found%' 
		   OR @ErrorMessage LIKE '%path%not%found%'
		BEGIN
			PRINT '⚠️ Issue: File path problem detected.';
			PRINT '   Make sure all CSV files are in the correct folder:';
			PRINT '   D:\Study\Udemy - The Complete SQL Bootcamp (30 Hours) Go from Zero to Hero 2025-6\25. SQL Data Warehouse Project\sql-data-warehouse-project\datasets\';
		END
		
		IF @ErrorMessage LIKE '%data conversion%' 
		   OR @ErrorMessage LIKE '%type%conversion%'
		   OR @ErrorMessage LIKE '%int%to%date%'
		BEGIN
			PRINT '⚠️ Issue: Data type mismatch detected.';
			PRINT '   Check CSV file for invalid data formats.';
			PRINT '   Common issues: Text in number columns, wrong date formats.';
		END
		
		IF @ErrorMessage LIKE '%permission%' 
		   OR @ErrorMessage LIKE '%access denied%'
		BEGIN
			PRINT '⚠️ Issue: Permission problem detected.';
			PRINT '   Make sure SQL Server has read access to the CSV files.';
		END
		
		PRINT '';
		PRINT 'Procedure Rolled Back.';
		PRINT 'Please fix the error and try again.';
		PRINT '';
		
		-- Re-throw the error to alert the calling application
		DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
		DECLARE @ErrorState INT = ERROR_STATE();
		RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
	END CATCH
END