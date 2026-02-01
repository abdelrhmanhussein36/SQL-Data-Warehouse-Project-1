--Find total products by category
SELECT COUNT(Product_name) products_by_category
	  ,Category
FROM gold.dim_prd
GROUP BY Category;