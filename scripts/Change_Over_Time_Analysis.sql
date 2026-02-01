--Change Over Time Analysis
SELECT
	YEAR(Order_date) order_year
	,FORMAT(Order_date,'MMM') order_Month
	,SUM(Sales_amount) Total_Salse
	,COUNT(DISTINCT Customer_Key) Total_Customers
	,SUM(quantity) Total_Quantity
FROM gold.fact_salse
	WHERE YEAR(Order_date) IS NOT NULL
	GROUP BY YEAR(Order_date),FORMAT(Order_date,'MMM')
	ORDER BY YEAR(Order_date),FORMAT(Order_date,'MMM') 