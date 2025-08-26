﻿--Check for Invalid Dates
SELECT
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_detail 
WHERE sls_order_dt <=0 
OR LEN(sls_order_dt) !=8 
OR sls_order_dt > 20500101 
OR sls_order_dt < 19000101

SELECT
NULLIF(sls_ship_dt, 0) sls_order_dt
FROM bronze.crm_sales_detail 
WHERE sls_ship_dt <=0 
OR LEN(sls_ship_dt) !=8 
OR sls_ship_dt > 20500101 
OR sls_ship_dt < 19000101

SELECT
NULLIF(sls_due_dt, 0) sls_due_dt
FROM bronze.crm_sales_detail 
WHERE sls_due_dt <=0 
OR LEN(sls_due_dt) !=8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101
-- Check for Invalid Date Orders
SELECT
*
FROM bronze.crm_sales_detail
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, Zero, Or Negative
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_detail
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

-- If sales is negative, zero, or null, derive it using Quantity and Price
-- If Price is zero or null, calculate it using Sales and Quantity
-- If Price is negative, convert it to a positive value	
-- Nhập dữ liệu

SELECT DISTINCT
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END as sls_sales,
CASE WHEN sls_price IS NULL or sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
	 ELSE sls_price
END as sls_price
FROM bronze.crm_sales_detail
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

-- CREATE TABLE
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;	
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)
-- INSERT DATA INTO crm_sales_details
INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
Select
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END as sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL or sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
	 ELSE sls_price
END as sls_price
FROM bronze.crm_sales_details


-- SILVER LAYER CHECK
-- Check for Invalid Date Orders
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, Zero, Or Negative
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price