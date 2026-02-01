/*
==============
Product Report
==============
Purpose:
	-This report consolidates key product metrics and behaviors.
Highlights:
	1. Gathers essential fields such as product name, category, subcategory, and cost.
	2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
	3. Aggregates product-level metrics:
		-total orders
		-total sales
		-total quantity sold
		-total customers (unique)
		-lifespan (in months)
	4. Calculates valuable KPIs:
		-recency (months since last sale)
		-average order revenue (AOR)
		-average monthly revenue
-------------------------------------------------------------------------------------------------
*/

/* ----------------------------------------------
1) Base Query: Retrieves core columns from tables
*/-----------------------------------------------
CREATE VIEW gold.Report_Products AS
WITH Base_Query AS(
SELECT sa.Product_key
	  ,pr.Product_name
	  ,pr.Category
	  ,pr.Subcategory
	  ,pr.Product_cost
	  ,sa.Order_date
	  ,sa.Quantity
	  ,sa.Order_number
	  ,sa.Sales_amount
	  ,sa.Customer_Key
FROM gold.fact_salse sa
LEFT JOIN gold.dim_prd pr
ON pr.Product_key=sa.Product_key)

/*
------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the Product level
------------------------------------------------------------------------
*/
,Product_Aggregation AS(
SELECT    Product_name
         ,Category
		 ,Subcategory
		 ,Product_cost
		 ,MIN(Order_date) first_Order_Date
		 ,MAX(Order_date) Last_Order_Date
		 ,COUNT(DISTINCT Customer_Key) Total_Customers
		 ,SUM(Sales_amount) Total_Sales
		 ,COUNT(DISTINCT Order_number) Total_orders
		 ,SUM(Quantity) Total_Quantity
		 ,DATEDIFF(MONTH,MIN(Order_date),MAX(Order_date)) Lifespan
FROM Base_Query
 GROUP BY Product_key,Product_name,Category,Subcategory,Product_cost)

 /*
 -------------------------------------------------------------
 3) Final Query: Combines all product results into one output
 -------------------------------------------------------------
 */
  SELECT 
          Product_name
         ,Category
		 ,Subcategory
		 ,Product_cost
		 ,Total_Customers
		 ,Total_Sales
		 ,CASE WHEN Total_Sales>50000 THEN' High-Performers'
		       WHEN Total_Sales>=10000 THEN' Mid-Range'
			   ELSE  'Low-Performers'
          END revenue_Segments
		 ,Total_orders
		 --Average Order Revenue (AOR)
		 ,CASE WHEN Total_orders=0 THEN 0
		       ELSE Total_Sales/Total_orders
		  END Avg_Order_Revenue
		 --Average Monthly Revenue
	     ,CASE WHEN Lifespan=0 THEN Total_Sales
		       ELSE Total_Sales/Lifespan
		  END Avg_Monthly_Revenue
		 ,Total_Quantity
		 ,Lifespan

 FROM Product_Aggregation
