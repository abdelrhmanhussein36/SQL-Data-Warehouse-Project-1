
--Find total customers by countries
SELECT COUNT(Customer_Number) customers_by_countries
	  ,Country
FROM gold.dim_customers
GROUP BY Country;