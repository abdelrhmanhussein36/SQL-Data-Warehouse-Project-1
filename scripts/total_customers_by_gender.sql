--Find total customers by gender
SELECT COUNT(Customer_Number) customers_by_gender
	  ,Gender
FROM gold.dim_customers
GROUP BY Gender;