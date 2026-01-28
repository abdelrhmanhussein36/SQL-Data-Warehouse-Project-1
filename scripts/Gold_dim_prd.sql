CREATE VIEW gold.dim_prd AS
SELECT 
       ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) Product_key
      ,pn.prd_id Product_id
      ,pn.prd_key Product_number
      ,pn.prd_nm Product_name
      ,pn.cat_id Category_id
      ,pc.CAT Category
      ,pc.SUBCAT Subcategory
      ,pc.MAINTENANCE Maintenance
      ,pn.prd_cost Product_cost
      ,pn.prd_line Product_line
      ,pn.prd_start_dt Start_date
  FROM silver.crm_prd_info pn
  LEFT JOIN silver.erp_PX_CAT_G1V2 pc
  ON pn.cat_id = pc.ID
  WHERE pn.prd_end_dt IS NULL --Filter out all hestorical data
