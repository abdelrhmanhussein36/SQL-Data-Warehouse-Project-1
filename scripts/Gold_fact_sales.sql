CREATE VIEW gold.fact_salse AS
SELECT sls_ord_num Order_number
      ,pr.Product_key 
      ,cu.Customer_Key
      ,sls_order_dt Order_date
      ,sls_ship_dt Shipping_date
      ,sls_due_dt Due_date
      ,sls_sales Sales_amount
      ,sls_quantity Quantity
      ,sls_price Price
  FROM silver.crm_sales_details sd
  LEFT JOIN gold.dim_prd pr
  ON sd.sls_prd_key = pr.Product_number
  LEFT JOIN gold.dim_customers cu
  ON sd.sls_cust_id = cu.Customer_id
