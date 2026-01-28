# SQL-Data-Warehouse-Project-1
Building a modern data warehouse with SQL Server, including ETL processes, data modeling, and analytics

📊 SQL Data Warehouse Project
Welcome to my SQL Data Warehouse Project! This project demonstrates a complete end-to-end data warehousing solution built using Microsoft SQL Server. It follows the Bronze → Silver → Gold layer architecture, a modern approach to data engineering and analytics.

🏗️ Project Overview
This project simulates a real-world data warehousing pipeline that:
-Extracts raw data from multiple sources (CRM & ERP systems)
-Transforms and cleans the data through different processing layers
-Creates structured, analysis-ready datasets
-Provides insights through aggregated views

📁 Project Structure
SQL-Data-Warehouse-Project-1/
│
├── 📂 Database Initialization/
│   └── Init_database.sql           # Creates database and schemas
│
├── 📂 Bronze Layer (Raw Data)/
│   └── bronze_Procedure.sql        # Loads raw CSV data into bronze tables
│
├── 📂 Silver Layer (Cleaned Data)/
│   └── Silver_DDL.sql              # Transforms bronze to silver layer
│
├── 📂 Gold Layer (Analytical Views)/
│   ├── Gold_dim_cust.sql           # Customer dimension view
│   ├── Gold_dim_prd.sql            # Product dimension view
│   └── Gold_fact_sales.sql         # Sales fact view
│
└── 📂 datasets/                    # Source data files
    ├── source_crm/
    │   ├── cust_info.csv
    │   ├── prd_info.csv
    │   └── sales_details.csv
    └── source_erp/
        ├── CUST_AZ12.csv
        ├── LOC_A101.csv
        └── PX_CAT_G1V2.csv
        
🎯 Project Architecture

1. Bronze Layer (Raw Ingestion)
Purpose: Initial landing zone for raw data
Technology: BULK INSERT from CSV files
Features:
Minimal transformation
Preserves source data format
Fast loading with performance optimizations

2. Silver Layer (Cleaned & Standardized)
Purpose: Data cleaning and standardization
Technology: Stored procedures with error handling
Features:
Data type standardization
Basic validation
Audit columns (dwh_create_date)
Staging tables for safe transformations

3. Gold Layer (Business-Ready Analytics)
Purpose: Business intelligence and reporting
Technology: SQL Views with dimensional modeling
Features:
Star Schema design
Dimension and fact tables
Business-friendly column names
Historical data filtering

📊 Gold Layer Components
🧑‍💼 Dim_Customers View (Gold_dim_cust.sql)
Creates a comprehensive customer dimension by joining CRM customer data with ERP demographic information.

Key Features:
Unified customer profile across systems
Gender standardization logic (falls back to ERP data when CRM is 'N/A')
Country information enrichment
Surrogate key generation for dimensional modeling

📦 Dim_Product View (Gold_dim_prd.sql)
Builds a product dimension with enriched category information and filtering for current products only.
Key Features:
Product categorization from ERP system
Active product filtering (prd_end_dt IS NULL)
Cost and product line information
Surrogate key for time-sensitive product tracking

💰 Fact_Sales View (Gold_fact_sales.sql)
Creates the central sales fact table by linking sales transactions with customer and product dimensions.
Key Features:
Fact table following star schema design
Foreign keys to dimension tables
Complete sales transaction details
Ready for time-based analysis and aggregations

🚀 Getting Started
Prerequisites
Microsoft SQL Server (2016 or higher)
SQL Server Management Studio (SSMS)
Read access to the dataset folder

Installation Steps

1-Initialize Database:
EXEC Init_database.sql;

2-Load Bronze Layer:
EXEC bronze.load_bronze;

3-Transform to Silver Layer:
EXEC Silver_DDL.sql;

4-Create Gold Layer Views:
-- Execute each view creation script
EXEC Gold_dim_cust.sql;
EXEC Gold_dim_prd.sql;
EXEC Gold_fact_sales.sql;

🔍 Sample Queries
--Top 5 Customers by Sales

SELECT TOP 5
    c.First_Name + ' ' + c.Last_Name AS Customer_Name,
    c.Country,
    SUM(f.Sales_amount) AS Total_Sales
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.Customer_Key = c.Customer_Key
GROUP BY c.First_Name, c.Last_Name, c.Country
ORDER BY Total_Sales DESC;

Product Category Performance

SELECT 
    p.Category,
    p.Subcategory,
    COUNT(DISTINCT f.Order_number) AS Order_Count,
    SUM(f.Quantity) AS Total_Quantity,
    SUM(f.Sales_amount) AS Total_Revenue
FROM gold.fact_sales f
JOIN gold.dim_prd p ON f.Product_key = p.Product_key
GROUP BY p.Category, p.Subcategory
ORDER BY Total_Revenue DESC;

⚙️ Performance Features
-Bulk Loading: Uses BULK INSERT with TABLOCK for fast data ingestion
-Error Handling: Comprehensive try-catch blocks with troubleshooting guidance
-Indexing Ready: Structure supports index creation for large datasets
-Date Intelligence: Proper date dimension relationships

🛠️ Troubleshooting
Common issues and solutions are embedded in the stored procedures, including:
-File path verification
-Data type conversion errors
-Permission issues
-Schema existence checks

📈 Business Value
This data warehouse enables:
-360° Customer View: Unified customer profiles across systems
-Product Performance Analysis: Track sales by category and product line
-Sales Trend Analysis: Time-based reporting capabilities
-Data Quality: Clean, standardized data for decision-making

📚 Learning Outcomes
Through this project, I've demonstrated:
-End-to-end ETL/ELT pipeline design
-Dimensional modeling (Star Schema)
-SQL Server performance optimization
-Error handling and data validation
-Multi-source data integration

🤝 Contributing
Feel free to fork this project and adapt it to your needs! Suggestions for improvements are always welcome.

📄 License
This project is for educational purposes as part of "The Complete SQL Bootcamp" course.

Built with ❤️ using SQL Server | Last Updated: Jan 206=26

👨‍💻 About Me
Civil Engineer turned Data Professional | With 5+ years in infrastructure engineering, I've transitioned to data engineering and GIS. Skilled in SQL, ETL pipelines, spatial analysis, and cloud platforms. Passionate about building data-driven solutions that bridge engineering precision with modern data analytics.

🔗 Connect: [LinkedIn](https://www.linkedin.com/in/abdelrhmanhussein/) | 📧 Email: abdelrahmanhussein36@gmail.com
