--And find the total number of customers by each group
WITH Customers_Segmentation AS (
SELECT 
	  sa.Customer_Key
	  ,SUM(sa.Sales_amount) Total_Spending
	  ,MIN(sa.Order_date) first_Order
	  ,MAX(sa.Order_date) Last_Order
	  ,DATEDIFF(MONTH,MIN(sa.Order_date),MAX(sa.Order_date)) Lifespan
	  ,CASE WHEN DATEDIFF(MONTH,MIN(sa.Order_date),MAX(sa.Order_date))>=12 AND SUM(sa.Sales_amount)>5000 THEN 'VIP'
			WHEN DATEDIFF(MONTH,MIN(sa.Order_date),MAX(sa.Order_date))>=12 AND SUM(sa.Sales_amount)<=5000 THEN 'Regular'
			WHEN DATEDIFF(MONTH,MIN(sa.Order_date),MAX(sa.Order_date)) <12  THEN 'NEW'
			ELSE 'N/A'
	   END Customer_Group
FROM gold.fact_salse sa
LEFT JOIN gold.dim_customers cu
ON cu.Customer_Key=sa.Customer_Key
GROUP BY sa.Customer_Key
)
SELECT COUNT(Customer_Key) Nr_Customers_Segment
	  ,Customer_Group
FROM Customers_Segmentation
GROUP BY Customer_Group