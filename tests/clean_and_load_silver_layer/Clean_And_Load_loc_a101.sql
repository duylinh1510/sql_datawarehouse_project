-- Data Standardization Or Consistency
SELECT DISTINCT cntry
FRom bronze.erp_loc_a101
ORDER BY cntry

-- INSERT DATA
INSERT INTO silver.erp_loc_a101 (cid, cntry)
Select
REPLACE(cid, '-', '') as cid,
CASE WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
	 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END as cntry
From bronze.erp_loc_a101



-- Data Standardization Or Consistency
SELECT DISTINCT cntry
FRom silver.erp_loc_a101
ORDER BY cntry