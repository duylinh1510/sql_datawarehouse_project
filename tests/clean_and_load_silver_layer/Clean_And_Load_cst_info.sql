--Check For Nulls Or Duplicate in Primary Key
-- Expectation: No Result
SELECT cst_id, COUNT(cst_id) as cnt_customers
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1 OR cst_id IS NULL

Select
*
FROM(
	SELECT
	*,
	ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date desc) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
) t Where flag_last = 1 and cst_id = 29466;

-- Check for unwanted spaces
-- Expectation: No Results
Select cst_firstname
FROM bronze.crm_cust_info
Where cst_firstname != TRIM(cst_firstname)

-- Check for unwanted spaces
-- Expectation: No Results
Select cst_lastname
FROM bronze.crm_cust_info
Where cst_lastname != TRIM(cst_lastname)

-- Check for unwanted spaces
-- Expectation: No Results
Select cst_gndr
FROM bronze.crm_cust_info
Where cst_gndr != TRIM(cst_gndr)

-- Data Standirdization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info

	INSERT INTO silver.crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
	--Chuẩn hóa dữ liệu, hiển thị full chữ cái không hiển thị viết tắt
	Select
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
		 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
		 WHEN UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
		 ELSE 'n/a'
	END cst_gndr,
	cst_create_date
FROM(
	SELECT
	*,
	ROW_NUMBER() OVER(Partition By cst_id Order By cst_create_date desc) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
) t Where flag_last = 1

-- Check Quality of Silver Layer
SELECT cst_id, COUNT(cst_id) as cnt_customers
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) > 1 OR cst_id IS NULL

-- Check for unwanted spaces
-- Expectation: No Results
Select cst_firstname
FROM silver.crm_cust_info
Where cst_firstname != TRIM(cst_firstname)

-- Check for unwanted spaces
-- Expectation: No Results
Select cst_lastname
FROM silver.crm_cust_info
Where cst_lastname != TRIM(cst_lastname)

-- Check for unwanted spaces
-- Expectation: No Results
Select cst_gndr
FROM silver.crm_cust_info
Where cst_gndr != TRIM(cst_gndr)

-- Data Standirdization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info