--Which Catigories Contribute the most to Overall Sales?
SELECT
	   pr.Category
	   ,SUM(sa.Sales_amount) SUM_Sales
	   ,CONCAT(ROUND((CAST(SUM(sa.Sales_amount) AS FLOAT))*100 /(SELECT SUM(Sales_amount) FROM gold.fact_salse),2),'%') Catigory_Percentage
	   ,CASE WHEN ROUND((CAST(SUM(sa.Sales_amount) AS FLOAT))*100 /(SELECT SUM(Sales_amount) FROM gold.fact_salse),2)>50 THEN 'Sales Is Highly Depends on This Product'
	         WHEN ROUND((CAST(SUM(sa.Sales_amount) AS FLOAT))*100 /(SELECT SUM(Sales_amount) FROM gold.fact_salse),2)>30 THEN 'Sales Is Moderately Depends on This Product'
			 WHEN ROUND((CAST(SUM(sa.Sales_amount) AS FLOAT))*100 /(SELECT SUM(Sales_amount) FROM gold.fact_salse),2)<10 THEN 'Sales Is Weak'
			 WHEN ROUND((CAST(SUM(sa.Sales_amount) AS FLOAT))*100 /(SELECT SUM(Sales_amount) FROM gold.fact_salse),2)>10 AND ROUND((CAST(SUM(sa.Sales_amount) AS FLOAT))*100 /(SELECT SUM(Sales_amount) FROM gold.fact_salse),2)<30 THEN 'Acceptable Salese'
			 END Salse_Evaluation
FROM gold.fact_salse sa
LEFT JOIN gold.dim_prd pr
ON pr.Product_key=sa.Product_key
GROUP BY pr.Category