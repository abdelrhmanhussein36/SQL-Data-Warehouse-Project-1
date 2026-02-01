/*Group customers into three segments based on their spending behavior:
VIP: Customers with at least 12 months of history and spending more than €5,000.
Regular: Customers with at least 12 months of history but spending €5,000 or less.
New: Customers with a lifespan less than 12 months.*/
SELECT   
	  CONCAT(cu.First_Name,' ',cu.Last_Name) full_Name
	  ,cu.Gender
	  ,cu.Country
	  ,sa.Customer_Key
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
GROUP BY sa.Customer_Key,cu.Country,cu.Gender,CONCAT(cu.First_Name,' ',cu.Last_Name)