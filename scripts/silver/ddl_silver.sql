/********************************************************************************************
 File:    silver_layer_tables.sql
 Author:  Linh
 Purpose: Script to (re)create tables in the [silver] schema for the Data Warehouse layer.

 ⚠️ IMPORTANT NOTES:
 1. This script contains `DROP TABLE` statements, meaning it will DELETE all existing data 
    in the silver.* tables before recreating them.
 2. Run only in DEV or TEST environments. Not recommended to run directly in PROD.
 3. Each table includes a `dwh_create_date` column with a default `GETDATE()` value to 
    track insertion timestamp.
 4. This script is intended to be used together with the stored procedure `silver.load_silver` 
    for loading data from the bronze layer into the silver layer.

 Last Updated: [dd/MM/yyyy]
********************************************************************************************/

IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status CHAR(10),
    cst_gndr CHAR(10),
    cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt datetime,
	prd_end_dt datetime,
	dwh_create_date DATETIME2 DEFAULT GETDATE()

);

IF OBJECT_ID ('silver.crm_sales_detail', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_detail;
CREATE TABLE silver.crm_sales_detail(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()

);

IF OBJECT_ID ('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()

);

IF OBJECT_ID ('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(10),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID ('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
