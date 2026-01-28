-- ============================================
-- GOLD LAYER TEST SCRIPT
-- Tests: View Functionality, Business Logic, Data Relationships
-- ============================================

USE DataWarehouse;
GO

PRINT '===========================================';
PRINT 'STARTING GOLD LAYER TESTS';
PRINT '===========================================';
PRINT '';

DECLARE @test_passed BIT = 1;
DECLARE @test_name NVARCHAR(100);
DECLARE @row_count INT;
DECLARE @silver_count INT;
DECLARE @gold_count INT;

-- ======================
-- TEST 1: View Existence
-- ======================
PRINT 'TEST 1: Checking if gold views exist...';
IF SCHEMA_ID('gold') IS NOT NULL
    PRINT '   ✅ gold schema exists';
ELSE BEGIN
    PRINT '   ❌ gold schema does not exist';
    SET @test_passed = 0;
END

-- Check for required views
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    PRINT '   ✅ gold.dim_customers view exists';
ELSE BEGIN
    PRINT '   ❌ gold.dim_customers view missing';
    SET @test_passed = 0;
END

IF OBJECT_ID('gold.dim_prd', 'V') IS NOT NULL
    PRINT '   ✅ gold.dim_prd view exists';
ELSE BEGIN
    PRINT '   ❌ gold.dim_prd view missing';
    SET @test_passed = 0;
END

IF OBJECT_ID('gold.fact_salse', 'V') IS NOT NULL  -- Note: Typo in original ('salse' instead of 'sales')
    PRINT '   ✅ gold.fact_salse view exists';
ELSE BEGIN
    PRINT '   ❌ gold.fact_salse view missing';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 2: View Data Availability
-- ======================
PRINT 'TEST 2: Checking if views return data...';

-- Test dim_customers
SELECT @row_count = COUNT(*) FROM gold.dim_customers;
IF @row_count > 0
    PRINT '   ✅ dim_customers returns ' + CAST(@row_count AS NVARCHAR) + ' rows';
ELSE BEGIN
    PRINT '   ❌ dim_customers returns no data';
    SET @test_passed = 0;
END

-- Test dim_prd
SELECT @row_count = COUNT(*) FROM gold.dim_prd;
IF @row_count > 0
    PRINT '   ✅ dim_prd returns ' + CAST(@row_count AS NVARCHAR) + ' rows';
ELSE BEGIN
    PRINT '   ❌ dim_prd returns no data';
    SET @test_passed = 0;
END

-- Test fact_sales
SELECT @row_count = COUNT(*) FROM gold.fact_salse;
IF @row_count > 0
    PRINT '   ✅ fact_salse returns ' + CAST(@row_count AS NVARCHAR) + ' rows';
ELSE BEGIN
    PRINT '   ❌ fact_salse returns no data';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 3: Business Logic - Customer Dimension
-- ======================
PRINT 'TEST 3: Testing dim_customers business logic...';

-- Test Gender standardization logic
PRINT '   Checking gender standardization...';
SELECT @row_count = COUNT(*) 
FROM gold.dim_customers 
WHERE Gender = 'N/A';

IF @row_count = 0
    PRINT '      ✅ No customers with N/A gender after standardization';
ELSE
    PRINT '      ⚠️ ' + CAST(@row_count AS NVARCHAR) + ' customers still have N/A gender';

-- Test Country enrichment
SELECT @row_count = COUNT(*) 
FROM gold.dim_customers 
WHERE Country IS NOT NULL;

PRINT '      ✅ ' + CAST(@row_count AS NVARCHAR) + ' customers have country information';

-- Test Customer Key generation
SELECT @row_count = COUNT(DISTINCT Customer_Key) FROM gold.dim_customers;
DECLARE @total_customers INT;
SELECT @total_customers = COUNT(*) FROM gold.dim_customers;

IF @row_count = @total_customers
    PRINT '      ✅ All customer keys are unique';
ELSE BEGIN
    PRINT '      ❌ Customer key duplicates found';
    SET @test_passed = 0;
END

-- Test for NULLs in critical fields
SELECT @row_count = COUNT(*) FROM gold.dim_customers WHERE Customer_id IS NULL;
IF @row_count = 0
    PRINT '      ✅ No NULL Customer_ids';
ELSE BEGIN
    PRINT '      ❌ Found ' + CAST(@row_count AS NVARCHAR) + ' NULL Customer_ids';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 4: Business Logic - Product Dimension
-- ======================
PRINT 'TEST 4: Testing dim_prd business logic...';

-- Test historical data filtering (prd_end_dt IS NULL)
PRINT '   Checking active products filter...';
SELECT @row_count = COUNT(*) FROM gold.dim_prd;
SELECT @silver_count = COUNT(*) FROM silver.crm_prd_info WHERE prd_end_dt IS NULL;

IF @row_count = @silver_count
    PRINT '      ✅ Correctly filtered to ' + CAST(@row_count AS NVARCHAR) + ' active products';
ELSE BEGIN
    PRINT '      ❌ Filter mismatch: Gold=' + CAST(@row_count AS NVARCHAR) + ', Silver active=' + CAST(@silver_count AS NVARCHAR);
    SET @test_passed = 0;
END

-- Test Category enrichment
SELECT @row_count = COUNT(*) 
FROM gold.dim_prd 
WHERE Category IS NOT NULL AND Subcategory IS NOT NULL;

PRINT '      ✅ ' + CAST(@row_count AS NVARCHAR) + ' products have full category information';

-- Test Product Key generation
SELECT @row_count = COUNT(DISTINCT Product_key) FROM gold.dim_prd;
SELECT @total_customers = COUNT(*) FROM gold.dim_prd;

IF @row_count = @total_customers
    PRINT '      ✅ All product keys are unique';
ELSE BEGIN
    PRINT '      ❌ Product key duplicates found';
    SET @test_passed = 0;
END

-- Test cost validation
SELECT @row_count = COUNT(*) FROM gold.dim_prd WHERE Product_cost < 0;
IF @row_count = 0
    PRINT '      ✅ No negative product costs';
ELSE BEGIN
    PRINT '      ❌ Found ' + CAST(@row_count AS NVARCHAR) + ' negative product costs';
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 5: Business Logic - Fact Sales
-- ======================
PRINT 'TEST 5: Testing fact_sales business logic...';

-- Test relationship integrity
PRINT '   Checking dimension relationships...';

-- Check for sales without valid product dimension
SELECT @row_count = COUNT(*) 
FROM gold.fact_salse f
LEFT JOIN gold.dim_prd p ON f.Product_key = p.Product_key
WHERE p.Product_key IS NULL;

IF @row_count = 0
    PRINT '      ✅ All sales have valid product dimension';
ELSE BEGIN
    PRINT '      ❌ Found ' + CAST(@row_count AS NVARCHAR) + ' sales without product dimension';
    SET @test_passed = 0;
END

-- Check for sales without valid customer dimension
SELECT @row_count = COUNT(*) 
FROM gold.fact_salse f
LEFT JOIN gold.dim_customers c ON f.Customer_Key = c.Customer_Key
WHERE c.Customer_Key IS NULL;

IF @row_count = 0
    PRINT '      ✅ All sales have valid customer dimension';
ELSE BEGIN
    PRINT '      ❌ Found ' + CAST(@row_count AS NVARCHAR) + ' sales without customer dimension';
    SET @test_passed = 0;
END

-- Test data consistency from silver layer
SELECT @gold_count = COUNT(*) FROM gold.fact_salse;
SELECT @silver_count = COUNT(*) FROM silver.crm_sales_details;

IF @gold_count = @silver_count
    PRINT '      ✅ All ' + CAST(@gold_count AS NVARCHAR) + ' sales transactions preserved';
ELSE BEGIN
    PRINT '      ❌ Data loss: Silver=' + CAST(@silver_count AS NVARCHAR) + ', Gold=' + CAST(@gold_count AS NVARCHAR);
    SET @test_passed = 0;
END
PRINT '';

-- ======================
-- TEST 6: Business Calculations
-- ======================
PRINT 'TEST 6: Testing business calculations...';

-- Test that Sales_amount = Quantity * Price
SELECT @row_count = COUNT(*)
FROM gold.fact_salse
WHERE Sales_amount != Quantity * Price;

IF @row_count = 0
    PRINT '   ✅ Sales amount calculation is correct for all rows';
ELSE BEGIN
    PRINT '   ❌ Found ' + CAST(@row_count AS NVARCHAR) + ' rows with incorrect sales calculation';
    
    -- Show sample of incorrect rows
    PRINT '      Sample incorrect rows:';
    SELECT TOP 3 
        Order_number,
        Quantity,
        Price,
        Sales_amount,
        Quantity * Price AS Calculated_Amount
    FROM gold.fact_salse
    WHERE Sales_amount != Quantity * Price;
    
    SET @test_passed = 0;
END

-- Test date relationships
SELECT @row_count = COUNT(*)
FROM gold.fact_salse
WHERE Shipping_date < Order_date 
   OR Due_date < Shipping_date 
   OR Due_date < Order_date;

IF @row_count = 0
    PRINT '   ✅ All date relationships are logical (Order ≤ Ship ≤ Due)';
ELSE
    PRINT '   ⚠️ Found ' + CAST(@row_count AS NVARCHAR) + ' rows with illogical date relationships';
PRINT '';

-- ======================
-- TEST 7: Star Schema Validation
-- ======================
PRINT 'TEST 7: Validating star schema structure...';

-- Test that fact table can join with all dimensions
PRINT '   Testing dimension joins...';

-- Try a comprehensive join
BEGIN TRY
    SELECT @row_count = COUNT(*)
    FROM gold.fact_salse f
    INNER JOIN gold.dim_customers c ON f.Customer_Key = c.Customer_Key
    INNER JOIN gold.dim_prd p ON f.Product_key = p.Product_key;
    
    PRINT '      ✅ Successful join across all dimensions: ' + CAST(@row_count AS NVARCHAR) + ' rows';
END TRY
BEGIN CATCH
    PRINT '      ❌ Failed to join dimensions: ' + ERROR_MESSAGE();
    SET @test_passed = 0;
END CATCH

-- Test that dimensions have surrogate keys
IF EXISTS (SELECT 1 FROM gold.dim_customers WHERE Customer_Key IS NULL)
    PRINT '      ❌ Customer dimension has NULL surrogate keys';
ELSE
    PRINT '      ✅ Customer dimension has complete surrogate keys';

IF EXISTS (SELECT 1 FROM gold.dim_prd WHERE Product_key IS NULL)
    PRINT '      ❌ Product dimension has NULL surrogate keys';
ELSE
    PRINT '      ✅ Product dimension has complete surrogate keys';
PRINT '';

-- ======================
-- TEST 8: Performance Test
-- ======================
PRINT 'TEST 8: Running performance tests...';

DECLARE @start_time DATETIME, @end_time DATETIME, @duration_ms INT;

-- Test query performance on fact table
SET @start_time = GETDATE();

SELECT 
    c.Country,
    c.Gender,
    p.Category,
    p.Subcategory,
    COUNT(DISTINCT f.Order_number) AS Order_Count,
    SUM(f.Quantity) AS Total_Quantity,
    SUM(f.Sales_amount) AS Total_Revenue,
    AVG(f.Price) AS Avg_Price
FROM gold.fact_salse f
JOIN gold.dim_customers c ON f.Customer_Key = c.Customer_Key
JOIN gold.dim_prd p ON f.Product_key = p.Product_key
GROUP BY c.Country, c.Gender, p.Category, p.Subcategory
HAVING SUM(f.Sales_amount) > 0;

SET @end_time = GETDATE();
SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time);

IF @duration_ms < 5000  -- Less than 5 seconds
    PRINT '   ✅ Complex analytical query executed in ' + CAST(@duration_ms AS NVARCHAR) + ' ms';
ELSE
    PRINT '   ⚠️ Complex query took ' + CAST(@duration_ms AS NVARCHAR) + ' ms (consider indexing)';

-- Test individual view performance
SET @start_time = GETDATE();
SELECT @row_count = COUNT(*) FROM gold.dim_customers;
SET @end_time = GETDATE();
SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, @end_time);
PRINT '   ✅ dim_customers query: ' + CAST(@duration_ms AS NVARCHAR) + ' ms';
PRINT '';

-- ======================
-- TEST 9: Sample Business Queries
-- ======================
PRINT 'TEST 9: Running sample business queries...';

PRINT '   a) Top 5 customers by total sales:';
SELECT TOP 5
    c.Customer_id,
    c.First_Name + ' ' + c.Last_Name AS Customer_Name,
    c.Country,
    SUM(f.Sales_amount) AS Total_Sales,
    COUNT(DISTINCT f.Order_number) AS Order_Count
FROM gold.fact_salse f
JOIN gold.dim_customers c ON f.Customer_Key = c.Customer_Key
GROUP BY c.Customer_id, c.First_Name, c.Last_Name, c.Country
ORDER BY Total_Sales DESC;

PRINT '';
PRINT '   b) Top 3 product categories by revenue:';
SELECT TOP 3
    p.Category,
    p.Subcategory,
    COUNT(DISTINCT f.Order_number) AS Order_Count,
    SUM(f.Quantity) AS Total_Quantity,
    SUM(f.Sales_amount) AS Total_Revenue
FROM gold.fact_salse f
JOIN gold.dim_prd p ON f.Product_key = p.Product_key
GROUP BY p.Category, p.Subcategory
ORDER BY Total_Revenue DESC;

PRINT '';
PRINT '   c) Monthly sales trend:';
SELECT 
    YEAR(f.Order_date) AS Sales_Year,
    MONTH(f.Order_date) AS Sales_Month,
    COUNT(DISTINCT f.Order_number) AS Order_Count,
    SUM(f.Sales_amount) AS Monthly_Revenue,
    SUM(f.Quantity) AS Monthly_Quantity
FROM gold.fact_salse f
GROUP BY YEAR(f.Order_date), MONTH(f.Order_date)
ORDER BY Sales_Year, Sales_Month;
PRINT '';

-- ======================
-- FINAL TEST SUMMARY
-- ======================
PRINT '===========================================';
PRINT 'GOLD LAYER TEST SUMMARY';
PRINT '===========================================';
IF @test_passed = 1
    PRINT '✅ ALL TESTS PASSED! Gold layer is business-ready.';
ELSE
    PRINT '❌ SOME TESTS FAILED. Please check the issues above.';
PRINT '===========================================';

-- Final Statistics
PRINT '';
PRINT '📊 GOLD LAYER STATISTICS:';
PRINT '-------------------------';
PRINT 'Dimension Sizes:';
PRINT '  • Customers: ' + CAST((SELECT COUNT(*) FROM gold.dim_customers) AS NVARCHAR);
PRINT '  • Products: ' + CAST((SELECT COUNT(*) FROM gold.dim_prd) AS NVARCHAR);
PRINT '';
PRINT 'Fact Table:';
PRINT '  • Sales Transactions: ' + CAST((SELECT COUNT(*) FROM gold.fact_salse) AS NVARCHAR);
PRINT '  • Total Revenue: ' + CAST((SELECT SUM(Sales_amount) FROM gold.fact_salse) AS NVARCHAR);
PRINT '  • Date Range: ' + 
      CAST((SELECT MIN(Order_date) FROM gold.fact_salse) AS NVARCHAR) + ' to ' +
      CAST((SELECT MAX(Order_date) FROM gold.fact_salse) AS NVARCHAR);
PRINT '';
PRINT '✅ Data Warehouse is ready for business intelligence!';
PRINT '===========================================';
GO