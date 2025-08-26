IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;	
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
)


-- Câu lệnh chính nạp dữ liệu vào bảng silver
INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
Select
	prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, -- Extract category ID
	SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, -- Extract product key
	prd_nm,
	ISNULL(prd_cost,0) as prd_cost,
	CASE UPPER(TRIM(prd_line))
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' then 'other Sales'
		 WHEN 'T' then 'Touring'
		 ELSE 'n/a'
	END AS prd_line, -- Map product line codes to descriptive values
	CAST(prd_start_dt AS DATE) as prd_start_dt,
	CAST(
		LEAD(prd_start_dt) OVER (PARTITION BY prd_key Order By prd_start_dt) -1 
		as DATE
	) AS prd_end_dt -- Calculate end date as one day before the next start date
FROM bronze.crm_prd_info

Select sls_prd_key
From bronze.crm_sales_detail


--Check for nulls or duplicates in primary key
-- Expectation: No Results
Select
prd_id,
COUNT(*) as cnt
From bronze.crm_prd_info
Group By prd_id
Having Count(*)>1 or prd_id is null

-- Check for unwanted spaces
-- Expectation: No Results
Select prd_nm
FROM bronze.crm_prd_info
Where prd_nm != TRIM(prd_nm)

-- Check for NULLS or Negative Numbers
-- Expectation: No Results
Select prd_cost
From bronze.crm_prd_info
Where prd_cost<0 or prd_cost is NUL

-- Data Standardization & Normalization
Select Distinct prd_line
From bronze.crm_prd_info

-- Check for invalid Date Orders
Select *
From bronze.crm_prd_info
Where prd_end_dt < prd_start_dt

-- Bây giờ phải chuẩn hóa prd_start_dt và prd_end_dt, end_date phải lớn hơn start_date và không có overlapping giữa các date
Select
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key Order By prd_start_dt) -1 as prd_end_dt_test
FROM bronze.crm_prd_info
Where prd_key IN('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


-------------------------------
-- Quality Checks SILVER TABLE
-------------------------------

--Check for nulls or duplicates in primary key
-- Expectation: No Results
Select
prd_id,
COUNT(*) as cnt
From silver.crm_prd_info
Group By prd_id
Having Count(*)>1 or prd_id is null

-- Check for unwanted spaces
-- Expectation: No Results
Select prd_nm
FROM silver.crm_prd_info
Where prd_nm != TRIM(prd_nm)

-- Check for NULLS or Negative Numbers
-- Expectation: No Results
Select prd_cost
From silver.crm_prd_info
Where prd_cost<0 or prd_cost is NULL

-- Data Standardization & Normalization
Select Distinct prd_line
From silver.crm_prd_info

-- Check for invalid Date Orders
Select *
From silver.crm_prd_info
Where prd_end_dt < prd_start_dt

Select * From silver.crm_prd_info