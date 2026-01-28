-- ============================================
-- BRONZE LAYER TEST SCRIPT
-- Tests: Data Existence, Structure, Basic Quality
-- ============================================

USE DataWarehouse;
GO

PRINT '===========================================';
PRINT 'STARTING BRONZE LAYER TESTS';
PRINT '===========================================';
PRINT '';

DECLARE @test_passed BIT = 1;
DECLARE @test_name NVARCHAR(100);
DECLARE @row_count INT;
DECLARE @column_count INT;

-- ======================
-- TEST 1: Schema Existence
-- ======================
PRINT 'TEST 1: Checking if bronze schema exists...';
IF SCHEMA_ID('bronze') IS NOT NULL
    PRINT '   ✅ PASS: bronze schema exists';
ELSE BEGIN
    PRINT '   ❌ FAIL: bronze schema does not exist';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 2: Table Existence
-- ======================
DECLARE @expected_tables TABLE (table_name NVARCHAR(100));
INSERT INTO @expected_tables VALUES
    ('crm_cust_info'),
    ('crm_prd_info'),
    ('crm_sales_details'),
    ('erp_CUST_AZ12'),
    ('erp_LOC_A101'),
    ('erp_PX_CAT_G1V2');

PRINT 'TEST 2: Checking if all bronze tables exist...';
DECLARE @missing_tables NVARCHAR(MAX) = '';

DECLARE table_cursor CURSOR FOR SELECT table_name FROM @expected_tables;
DECLARE @current_table NVARCHAR(100);

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @current_table;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('bronze.' + @current_table) IS NOT NULL
        PRINT '   ✅ ' + @current_table + ' exists';
    ELSE BEGIN
        PRINT '   ❌ ' + @current_table + ' missing';
        SET @missing_tables = @missing_tables + @current_table + ', ';
        SET @test_passed = 0;
    END
    FETCH NEXT FROM table_cursor INTO @current_table;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

IF LEN(@missing_tables) > 0
    PRINT '   MISSING TABLES: ' + LEFT(@missing_tables, LEN(@missing_tables) - 1);
PRINT '';

-- ======================
-- TEST 3: Data Volume Checks
-- ======================
PRINT 'TEST 3: Checking data volume (minimum rows)...';

DECLARE @min_rows_expected INT = 1;

-- Test CRM Customer Info
SELECT @row_count = COUNT(*) FROM bronze.crm_cust_info;
IF @row_count >= @min_rows_expected
    PRINT '   ✅ crm_cust_info has ' + CAST(@row_count AS NVARCHAR) + ' rows';
ELSE BEGIN
    PRINT '   ❌ crm_cust_info has only ' + CAST(@row_count AS NVARCHAR) + ' rows';
    SET @test_passed = 0;
END

-- Test CRM Product Info
SELECT @row_count = COUNT(*) FROM bronze.crm_prd_info;
IF @row_count >= @min_rows_expected
    PRINT '   ✅ crm_prd_info has ' + CAST(@row_count AS NVARCHAR) + ' rows';
ELSE BEGIN
    PRINT '   ❌ crm_prd_info has only ' + CAST(@row_count AS NVARCHAR) + ' rows';
    SET @test_passed = 0;
END

-- Test CRM Sales Details
SELECT @row_count = COUNT(*) FROM bronze.crm_sales_details;
IF @row_count >= @min_rows_expected
    PRINT '   ✅ crm_sales_details has ' + CAST(@row_count AS NVARCHAR) + ' rows';
ELSE BEGIN
    PRINT '   ❌ crm_sales_details has only ' + CAST(@row_count AS NVARCHAR) + ' rows';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 4: Column Structure Checks
-- ======================
PRINT 'TEST 4: Checking table column counts...';

-- Expected column counts
IF (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'crm_cust_info') = 7
    PRINT '   ✅ crm_cust_info has 7 columns';
ELSE BEGIN
    PRINT '   ❌ crm_cust_info column count mismatch';
    SET @test_passed = 0;
END

IF (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'crm_prd_info') = 7
    PRINT '   ✅ crm_prd_info has 7 columns';
ELSE BEGIN
    PRINT '   ❌ crm_prd_info column count mismatch';
    SET @test_passed = 0;
END

IF (SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'crm_sales_details') = 9
    PRINT '   ✅ crm_sales_details has 9 columns';
ELSE BEGIN
    PRINT '   ❌ crm_sales_details column count mismatch';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 5: Data Type Checks
-- ======================
PRINT 'TEST 5: Checking critical data types...';

-- Check date columns
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'crm_cust_info' 
    AND COLUMN_NAME = 'cst_create_date' AND DATA_TYPE = 'date'
)
    PRINT '   ✅ crm_cust_info.cst_create_date is DATE type';
ELSE BEGIN
    PRINT '   ❌ crm_cust_info.cst_create_date has wrong data type';
    SET @test_passed = 0;
END

-- Check numeric columns
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = 'crm_prd_info' 
    AND COLUMN_NAME = 'prd_cost' AND DATA_TYPE IN ('decimal', 'numeric')
)
    PRINT '   ✅ crm_prd_info.prd_cost is DECIMAL type';
ELSE BEGIN
    PRINT '   ❌ crm_prd_info.prd_cost has wrong data type';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 6: Primary Key Uniqueness
-- ======================
PRINT 'TEST 6: Checking key uniqueness...';

-- Check for duplicate customer IDs
SELECT @row_count = COUNT(DISTINCT cst_id) FROM bronze.crm_cust_info;
DECLARE @total_rows INT;
SELECT @total_rows = COUNT(*) FROM bronze.crm_cust_info;

IF @row_count = @total_rows
    PRINT '   ✅ crm_cust_info.cst_id is unique (' + CAST(@row_count AS NVARCHAR) + ' distinct values)';
ELSE BEGIN
    PRINT '   ⚠️ crm_cust_info.cst_id has duplicates: ' + CAST(@total_rows AS NVARCHAR) + ' rows, ' + CAST(@row_count AS NVARCHAR) + ' distinct';
    SET @test_passed = 0;
END

-- Check for duplicate product IDs
SELECT @row_count = COUNT(DISTINCT prd_id) FROM bronze.crm_prd_info;
SELECT @total_rows = COUNT(*) FROM bronze.crm_prd_info;

IF @row_count = @total_rows
    PRINT '   ✅ crm_prd_info.prd_id is unique (' + CAST(@row_count AS NVARCHAR) + ' distinct values)';
ELSE BEGIN
    PRINT '   ⚠️ crm_prd_info.prd_id has duplicates: ' + CAST(@total_rows AS NVARCHAR) + ' rows, ' + CAST(@row_count AS NVARCHAR) + ' distinct';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 7: Data Quality - NULL Checks
-- ======================
PRINT 'TEST 7: Checking for NULLs in critical columns...';

-- Check for NULLs in customer key
SELECT @row_count = COUNT(*) FROM bronze.crm_cust_info WHERE cst_key IS NULL;
IF @row_count = 0
    PRINT '   ✅ crm_cust_info.cst_key has no NULLs';
ELSE BEGIN
    PRINT '   ❌ crm_cust_info.cst_key has ' + CAST(@row_count AS NVARCHAR) + ' NULL values';
    SET @test_passed = 0;
END

-- Check for NULLs in product key
SELECT @row_count = COUNT(*) FROM bronze.crm_prd_info WHERE prd_key IS NULL;
IF @row_count = 0
    PRINT '   ✅ crm_prd_info.prd_key has no NULLs';
ELSE BEGIN
    PRINT '   ❌ crm_prd_info.prd_key has ' + CAST(@row_count AS NVARCHAR) + ' NULL values';
    SET @test_passed = 0;
END

-- Check for NULLs in sales order number
SELECT @row_count = COUNT(*) FROM bronze.crm_sales_details WHERE sls_ord_num IS NULL;
IF @row_count = 0
    PRINT '   ✅ crm_sales_details.sls_ord_num has no NULLs';
ELSE BEGIN
    PRINT '   ❌ crm_sales_details.sls_ord_num has ' + CAST(@row_count AS NVARCHAR) + ' NULL values';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- FINAL TEST SUMMARY
-- ======================
PRINT '===========================================';
PRINT 'BRONZE LAYER TEST SUMMARY';
PRINT '===========================================';
IF @test_passed = 1
    PRINT '✅ ALL TESTS PASSED! Bronze layer is ready.';
ELSE
    PRINT '❌ SOME TESTS FAILED. Please check the issues above.';
PRINT '===========================================';
GO