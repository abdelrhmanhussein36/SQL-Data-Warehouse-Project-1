--What is the total revenue generated for each category?
SELECT SUM(sa.Price)-SUM(pr.Product_cost) revenue_for_each_category
	  ,Category
FROM gold.dim_prd pr
 JOIN gold.fact_salse sa
ON sa.Product_key=pr.Product_key
GROUP BY pr.Category;