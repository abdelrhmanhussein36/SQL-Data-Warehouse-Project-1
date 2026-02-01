/*Cumulative Analysis
=======================*/
--Calculate The Total Sales Per Month and The Running Total of Sales Over Time
SELECT
	FORMAT(order_Month,'MMM') order_Month
	,YEAR(order_Month) Year_date
	,Total_Salse
	,AVG(AVG_Price) OVER( ORDER BY order_Month) Moving_avarage_Price
	,SUM(Total_Salse) OVER(PARTITION BY YEAR(order_Month) ORDER BY order_Month) Running_Total_Sales_YEAR
	,SUM(Total_Salse) OVER( ORDER BY order_Month) Running_Total_Sales
FROM(
		SELECT
			DATEFROMPARTS(YEAR(Order_date), MONTH(Order_date), 1)  order_Month
			,SUM(Sales_amount) Total_Salse
			,AVG(price) AVG_Price
		FROM gold.fact_salse
		WHERE Order_date IS NOT NULL
		GROUP BY YEAR(Order_date),MONTH(Order_date) )t 