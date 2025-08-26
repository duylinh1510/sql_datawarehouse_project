
-- Chuẩn hóa dữ liệu customer id về giống với bảng crm_cust_info để 2 bảng có thể join nhau
SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	  ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- Identify Out-of-Range Dates
-- Birthdays too old or Birthdays in the future are unacceptable
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
Select DISTINCT gen
FROM bronze.erp_cust_az12

-- INSERT Data from bronze.erp_cust_az12 into silver.erp_cust_az12
INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) --Remove 'NAS' prefix if present
	 ELSE cid
END cid,
CASE WHEN bdate > GETDATE() Then NULL --Birthday can't be in the future
	 ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen --Normalize gender values and handle unknown cases
FROM bronze.erp_cust_az12


---------------------------
-- Recheck Data from silver layer
-- Check if any customer id in silver.erp_cust_az12 doesn't match with silver.crm_cust_info
SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	  ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- Identify Out-of-Range Dates
-- Birthdays too old or Birthdays in the future are unacceptable
SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate <'1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
Select DISTINCT gen
FROM silver.erp_cust_az12
