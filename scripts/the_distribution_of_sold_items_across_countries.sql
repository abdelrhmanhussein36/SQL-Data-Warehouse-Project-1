--What is the distribution of sold items across countries?
SELECT SUM(sa.Quantity) distribution_countries
	  ,cu.Country
FROM  gold.fact_salse sa
 LEFT JOIN gold.dim_customers cu
ON sa.Customer_Key=cu.Customer_Key
GROUP BY cu.Country;