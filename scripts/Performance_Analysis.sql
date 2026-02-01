/**Performance Analysis**
==========================
Analyze the yearly performance of products by comparing their sales to both
the average sales performance of the product and the previous year's sales*/

SELECT 
	  sa.Product_key
	  ,pr.Product_name
	  ,YEAR(sa.order_date) YEAR_Date
	  ,SUM(sa.Sales_amount) SUM_Sales
	  ,AVG(sa.Sales_amount) Avarage_Sales
	  ,SUM(sa.Sales_amount) - AVG(sa.Sales_amount) Comparing_to_Avarage
	  ,CASE WHEN SUM(sa.Sales_amount) - AVG(sa.Sales_amount) > 0 THEN 'Above Avarage'
			WHEN SUM(sa.Sales_amount) - AVG(sa.Sales_amount) < 0 THEN 'Below Avarage'
			WHEN SUM(sa.Sales_amount) - AVG(sa.Sales_amount) = 0 THEN 'Equals Avarage'
			ELSE 'N/A' 
      END CASE_AVG
	  ,CASE WHEN LEAD(SUM(sa.Sales_amount),1) OVER (PARTITION BY sa.Product_key ORDER BY sa.Product_key ) IS NULL THEN 0
	   ELSE LEAD(SUM(sa.Sales_amount),1) OVER (PARTITION BY sa.Product_key ORDER BY YEAR(sa.order_date) )
	   END Previous_Year_Sales
	   ,COALESCE(SUM(sa.Sales_amount)-LEAD(SUM(sa.Sales_amount),1) OVER (PARTITION BY sa.Product_key ORDER BY sa.Product_key ),0) Comparing_to_Previouse_Year
	   ,CASE WHEN COALESCE(SUM(sa.Sales_amount)-LEAD(SUM(sa.Sales_amount),1) OVER (PARTITION BY sa.Product_key ORDER BY sa.Product_key ),0)>0 THEN 'Profit'
		     WHEN COALESCE(SUM(sa.Sales_amount)-LEAD(SUM(sa.Sales_amount),1) OVER (PARTITION BY sa.Product_key ORDER BY sa.Product_key ),0)<0 THEN 'Loss'
			 ELSE 'No Change'
			 END CASE_Prevouse_YEAR
FROM gold.fact_salse sa 
	LEFT JOIN gold.dim_prd pr
	ON pr.Product_key=sa.Product_key
	WHERE YEAR(order_date) IS NOT NULL
	GROUP BY pr.Product_name,sa.Product_key,YEAR(order_date)