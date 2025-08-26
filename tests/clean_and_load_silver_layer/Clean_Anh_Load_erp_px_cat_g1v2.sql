-- Check for unwanted spaces
Select cat
From bronze.erp_px_cat_g1v2
Where cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
Select DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2


INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
Select
id,
cat,
subcat,
maintenance
From bronze.erp_px_cat_g1v2

-- Check for unwanted spaces
Select cat
From silver.erp_px_cat_g1v2
Where cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
Select DISTINCT maintenance
FROM silver.erp_px_cat_g1v2