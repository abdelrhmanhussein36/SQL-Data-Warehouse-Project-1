-- ============================================
-- SILVER LAYER TEST SCRIPT
-- Tests: Data Transformation, Quality, Relationships
-- ============================================

USE DataWarehouse;
GO

PRINT '===========================================';
PRINT 'STARTING SILVER LAYER TESTS';
PRINT '===========================================';
PRINT '';

DECLARE @test_passed BIT = 1;
DECLARE @test_name NVARCHAR(100);
DECLARE @row_count INT;
DECLARE @bronze_count INT;
DECLARE @silver_count INT;

-- ======================
-- TEST 1: Schema and Table Existence
-- ======================
PRINT 'TEST 1: Checking if silver schema and tables exist...';
IF SCHEMA_ID('silver') IS NOT NULL
    PRINT '   ✅ silver schema exists';
ELSE BEGIN
    PRINT '   ❌ silver schema does not exist';
    SET @test_passed = 0;
END

-- Check all silver tables
DECLARE @silver_tables TABLE (table_name NVARCHAR(100));
INSERT INTO @silver_tables VALUES
    ('crm_cust_info'),
    ('crm_prd_info'),
    ('crm_sales_details'),
    ('erp_CUST_AZ12'),
    ('erp_LOC_A101'),
    ('erp_PX_CAT_G1V2');

DECLARE @missing_silver_tables NVARCHAR(MAX) = '';
DECLARE silver_cursor CURSOR FOR SELECT table_name FROM @silver_tables;
DECLARE @current_silver_table NVARCHAR(100);

OPEN silver_cursor;
FETCH NEXT FROM silver_cursor INTO @current_silver_table;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('silver.' + @current_silver_table) IS NOT NULL
        PRINT '   ✅ silver.' + @current_silver_table + ' exists';
    ELSE BEGIN
        PRINT '   ❌ silver.' + @current_silver_table + ' missing';
        SET @missing_silver_tables = @missing_silver_tables + @current_silver_table + ', ';
        SET @test_passed = 0;
    END
    FETCH NEXT FROM silver_cursor INTO @current_silver_table;
END

CLOSE silver_cursor;
DEALLOCATE silver_cursor;
PRINT '';

-- ======================
-- TEST 2: Audit Column Check
-- ======================
PRINT 'TEST 2: Checking for audit columns (dwh_create_date)...';

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'crm_cust_info' 
    AND COLUMN_NAME = 'dwh_create_date'
)
    PRINT '   ✅ silver.crm_cust_info has dwh_create_date';
ELSE BEGIN
    PRINT '   ❌ silver.crm_cust_info missing dwh_create_date';
    SET @test_passed = 0;
END

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'crm_prd_info' 
    AND COLUMN_NAME = 'dwh_create_date'
)
    PRINT '   ✅ silver.crm_prd_info has dwh_create_date';
ELSE BEGIN
    PRINT '   ❌ silver.crm_prd_info missing dwh_create_date';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 3: Data Completeness (Bronze vs Silver)
-- ======================
PRINT 'TEST 3: Checking data completeness (no data loss from Bronze)...';

-- CRM Customer Info
SELECT @bronze_count = COUNT(*) FROM bronze.crm_cust_info;
SELECT @silver_count = COUNT(*) FROM silver.crm_cust_info;

IF @bronze_count = @silver_count
    PRINT '   ✅ CRM Customer Info: All ' + CAST(@bronze_count AS NVARCHAR) + ' rows migrated';
ELSE BEGIN
    PRINT '   ❌ CRM Customer Info: Bronze=' + CAST(@bronze_count AS NVARCHAR) + ', Silver=' + CAST(@silver_count AS NVARCHAR);
    SET @test_passed = 0;
END

-- CRM Product Info
SELECT @bronze_count = COUNT(*) FROM bronze.crm_prd_info;
SELECT @silver_count = COUNT(*) FROM silver.crm_prd_info;

IF @bronze_count = @silver_count
    PRINT '   ✅ CRM Product Info: All ' + CAST(@bronze_count AS NVARCHAR) + ' rows migrated';
ELSE BEGIN
    PRINT '   ❌ CRM Product Info: Bronze=' + CAST(@bronze_count AS NVARCHAR) + ', Silver=' + CAST(@silver_count AS NVARCHAR);
    SET @test_passed = 0;
END

-- CRM Sales Details
SELECT @bronze_count = COUNT(*) FROM bronze.crm_sales_details;
SELECT @silver_count = COUNT(*) FROM silver.crm_sales_details;

IF @bronze_count = @silver_count
    PRINT '   ✅ CRM Sales Details: All ' + CAST(@bronze_count AS NVARCHAR) + ' rows migrated';
ELSE BEGIN
    PRINT '   ❌ CRM Sales Details: Bronze=' + CAST(@bronze_count AS NVARCHAR) + ', Silver=' + CAST(@silver_count AS NVARCHAR);
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 4: Data Type Transformation
-- ======================
PRINT 'TEST 4: Checking data type transformations...';

-- Check date conversion from INT to DATE in sales details
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'crm_sales_details' 
    AND COLUMN_NAME = 'sls_order_dt' AND DATA_TYPE = 'date'
)
BEGIN
    -- Also check that the data is valid
    SELECT @row_count = COUNT(*) FROM silver.crm_sales_details 
    WHERE sls_order_dt IS NULL OR sls_order_dt = '1900-01-01';
    
    IF @row_count = 0
        PRINT '   ✅ sales dates converted to DATE type and valid';
    ELSE
        PRINT '   ⚠️ sales dates converted but ' + CAST(@row_count AS NVARCHAR) + ' invalid dates found';
END
ELSE BEGIN
    PRINT '   ❌ sales dates not converted to DATE type';
    SET @test_passed = 0;
END

-- Check DATETIME to DATE conversion in product info
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = 'crm_prd_info' 
    AND COLUMN_NAME = 'prd_start_dt' AND DATA_TYPE = 'date'
)
    PRINT '   ✅ product dates converted from DATETIME to DATE type';
ELSE BEGIN
    PRINT '   ❌ product dates not converted to DATE type';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 5: Data Quality - Valid Ranges
-- ======================
PRINT 'TEST 5: Checking data validity and ranges...';

-- Check for negative sales amounts
SELECT @row_count = COUNT(*) FROM silver.crm_sales_details WHERE sls_sales < 0;
IF @row_count = 0
    PRINT '   ✅ No negative sales amounts';
ELSE BEGIN
    PRINT '   ❌ Found ' + CAST(@row_count AS NVARCHAR) + ' negative sales amounts';
    SET @test_passed = 0;
END

-- Check for negative quantities
SELECT @row_count = COUNT(*) FROM silver.crm_sales_details WHERE sls_quantity < 0;
IF @row_count = 0
    PRINT '   ✅ No negative quantities';
ELSE BEGIN
    PRINT '   ❌ Found ' + CAST(@row_count AS NVARCHAR) + ' negative quantities';
    SET @test_passed = 0;
END

-- Check for reasonable product costs
SELECT @row_count = COUNT(*) FROM silver.crm_prd_info WHERE prd_cost < 0 OR prd_cost > 1000000;
IF @row_count = 0
    PRINT '   ✅ All product costs are within reasonable range (0-1,000,000)';
ELSE BEGIN
    PRINT '   ⚠️ Found ' + CAST(@row_count AS NVARCHAR) + ' product costs outside reasonable range';
END
PRINT '';

-- ======================
-- TEST 6: Referential Integrity
-- ======================
PRINT 'TEST 6: Checking referential integrity...';

-- Check if all sales have valid customer IDs
SELECT @row_count = COUNT(*) 
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_cust_info c ON s.sls_cust_id = c.cst_id
WHERE c.cst_id IS NULL;

IF @row_count = 0
    PRINT '   ✅ All sales have valid customer IDs';
ELSE BEGIN
    PRINT '   ❌ Found ' + CAST(@row_count AS NVARCHAR) + ' sales with invalid customer IDs';
    SET @test_passed = 0;
END

-- Check if all sales have valid product keys
SELECT @row_count = COUNT(*) 
FROM silver.crm_sales_details s
LEFT JOIN silver.crm_prd_info p ON s.sls_prd_key = p.prd_key
WHERE p.prd_key IS NULL;

IF @row_count = 0
    PRINT '   ✅ All sales have valid product keys';
ELSE BEGIN
    PRINT '   ⚠️ Found ' + CAST(@row_count AS NVARCHAR) + ' sales with invalid product keys (might be ERP products)';
END
PRINT '';

-- ======================
-- TEST 7: ERP Data Quality
-- ======================
PRINT 'TEST 7: Checking ERP data integration...';

-- Check ERP customer data
SELECT @row_count = COUNT(*) FROM silver.erp_CUST_AZ12;
IF @row_count > 0
    PRINT '   ✅ ERP Customer data loaded: ' + CAST(@row_count AS NVARCHAR) + ' rows';
ELSE BEGIN
    PRINT '   ❌ ERP Customer data is empty';
    SET @test_passed = 0;
END

-- Check ERP location data
SELECT @row_count = COUNT(*) FROM silver.erp_LOC_A101;
IF @row_count > 0
    PRINT '   ✅ ERP Location data loaded: ' + CAST(@row_count AS NVARCHAR) + ' rows';
ELSE BEGIN
    PRINT '   ❌ ERP Location data is empty';
    SET @test_passed = 0;
END

-- Check ERP product category data
SELECT @row_count = COUNT(*) FROM silver.erp_PX_CAT_G1V2;
IF @row_count > 0
    PRINT '   ✅ ERP Product Category data loaded: ' + CAST(@row_count AS NVARCHAR) + ' rows';
ELSE BEGIN
    PRINT '   ❌ ERP Product Category data is empty';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 8: Data Consistency Checks
-- ======================
PRINT 'TEST 8: Performing data consistency checks...';

-- Check for sales with ship date before order date
SELECT @row_count = COUNT(*) 
FROM silver.crm_sales_details 
WHERE sls_ship_dt < sls_order_dt;

IF @row_count = 0
    PRINT '   ✅ No sales shipped before order date';
ELSE BEGIN
    PRINT '   ⚠️ Found ' + CAST(@row_count AS NVARCHAR) + ' sales shipped before order date';
    SET @test_passed = 0;
END

-- Check for sales with due date before ship date
SELECT @row_count = COUNT(*) 
FROM silver.crm_sales_details 
WHERE sls_due_dt < sls_ship_dt;

IF @row_count = 0
    PRINT '   ✅ No sales due before ship date';
ELSE BEGIN
    PRINT '   ⚠️ Found ' + CAST(@row_count AS NVARCHAR) + ' sales due before ship date';
END
PRINT '';

-- ======================
-- FINAL TEST SUMMARY
-- ======================
PRINT '===========================================';
PRINT 'SILVER LAYER TEST SUMMARY';
PRINT '===========================================';
IF @test_passed = 1
    PRINT '✅ ALL TESTS PASSED! Silver layer transformations are complete and valid.';
ELSE
    PRINT '❌ SOME TESTS FAILED. Please check the issues above.';
PRINT '===========================================';

-- Additional diagnostic query
PRINT '';
PRINT '📊 DIAGNOSTIC SUMMARY:';
PRINT '----------------------';
SELECT 
    'silver.crm_cust_info' AS Table_Name, 
    COUNT(*) AS Row_Count,
    MIN(dwh_create_date) AS First_Load,
    MAX(dwh_create_date) AS Last_Load
FROM silver.crm_cust_info
UNION ALL
SELECT 
    'silver.crm_prd_info', 
    COUNT(*),
    MIN(dwh_create_date),
    MAX(dwh_create_date)
FROM silver.crm_prd_info
UNION ALL
SELECT 
    'silver.crm_sales_details', 
    COUNT(*),
    MIN(dwh_create_date),
    MAX(dwh_create_date)
FROM silver.crm_sales_details;
GO