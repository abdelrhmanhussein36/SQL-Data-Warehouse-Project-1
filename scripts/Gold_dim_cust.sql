
CREATE VIEW gold.dim_customers AS
    SELECT ROW_NUMBER() OVER(ORDER BY cst_id) Customer_Key
          ,ci.cst_id Customer_id
          ,ci.cst_key Customer_Number
          ,ci.cst_firstname First_Name
          ,ci.cst_lastname Last_Name
          ,la.CNTRY Country
          ,ci.cst_marital_status Marital_Status
          ,CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
                ELSE COALESCE(ca.GEN,'N/A')
           END Gender
           ,ca.BDATE Birth_Date
          ,ci.cst_create_date Create_Date
      FROM silver.crm_cust_info ci
      LEFT JOIN silver.erp_CUST_AZ12 ca
      ON ci.cst_key = ca.CID
       LEFT JOIN silver.erp_LOC_A101 la
      ON ci.cst_key = la.CID