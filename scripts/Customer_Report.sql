/*
==================
Customer Report
==================
Purpose:
	-This report consolidates key customer metrics and behaviors

Highlights:
	1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
	3. Aggregates customer-level metrics:
		-total orders
		-total sales
		-total quantity purchased
		-total products
		-lifespan (in months)
	4. Calculates valuable KPIs:
	recency (months since last order)
	average order value
	average monthly spend*/

/* ----------------------------------------------
1) Base Query: Retrieves core columns from tables
*/-----------------------------------------------
CREATE VIEW gold.Report_Customers AS
WITH Base_Query AS(
	SELECT
		  sa.Order_number
		 ,sa.Product_key
		 ,sa.Order_date
		 ,sa.Sales_amount
		 ,sa.Quantity
		 ,cu.Customer_Key
		 ,cu.Customer_Number
		 ,CONCAT(cu.First_Name,' ',cu.Last_Name) full_Name
		 ,DATEDIFF(YEAR,cu.Birth_Date,GETDATE()) Age
		 ,cu.Country
	FROM gold.fact_salse sa
	LEFT JOIN gold.dim_customers cu
	ON cu.Customer_Key=sa.Customer_Key
	WHERE sa.Order_date IS NOT NULL)
/*
------------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
------------------------------------------------------------------------
*/
,Customer_Aggrigation AS(
SELECT 
		 Customer_Key
		 ,Customer_Number
		 ,full_Name
		 ,Age
		 ,MIN(Order_date) first_Order_Date
		 ,MAX(Order_date) Last_Order_Date
		 ,COUNT(DISTINCT Order_number) Total_orders
		 ,SUM(Sales_amount) Total_Sales
		 ,SUM(Quantity) Total_Quantity
		 ,COUNT(DISTINCT Product_key) Total_Products
		 ,DATEDIFF(MONTH,MIN(Order_date),MAX(Order_date)) Lifespan
FROM Base_Query
		 GROUP BY Customer_Key,Customer_Number,full_Name,Age )

		 SELECT Customer_Key
			   ,Customer_Number
			   ,full_Name
			   ,Age
	    	   ,CASE WHEN Age<20 THEN 'Under 20' 
					 WHEN Age BETWEEN 20 AND 29 THEN '20-29' 
					 WHEN Age BETWEEN 30 AND 39 THEN '30-39'
					 WHEN Age BETWEEN 40 AND 49 THEN '40-49' 
					 ELSE 'Above 50'
					 END Age_Group
			   ,CASE WHEN DATEDIFF(MONTH,first_Order_Date,Last_Order_Date)>=12 AND Total_Sales>5000 THEN 'VIP'
			         WHEN DATEDIFF(MONTH,first_Order_Date,Last_Order_Date)>=12 AND Total_Sales<=5000 THEN 'Regular'
			         WHEN DATEDIFF(MONTH,first_Order_Date,Last_Order_Date) <12  THEN 'NEW'
			         ELSE 'N/A'
	            END Customer_Segment
				,DATEDIFF(MONTH,Last_Order_Date,GETDATE()) Recency
			   ,Total_orders
		       ,Total_Sales
		       ,Total_Quantity
		       ,Total_Products
		       ,Lifespan
--Compute average order value (AVO)
			   ,CASE WHEN Total_orders=0 THEN 0
				ELSE Total_Sales/Total_orders
			    END AVG_Order_Value
--Compute average Monthly Spend
			   ,CASE WHEN Lifespan=0 THEN Total_Sales
				ELSE Total_Sales/Lifespan
			    END AVG_Monthly_Spend
		 FROM Customer_Aggrigation