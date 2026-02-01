--What is the average costs in each category?
SELECT AVG(Product_cost) AVG_products_by_category
	  ,Category
FROM gold.dim_prd
GROUP BY Category;