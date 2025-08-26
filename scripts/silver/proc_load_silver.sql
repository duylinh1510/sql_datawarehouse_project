/********************************************************************************************
 File:    load_silver_procedure.sql
 Author:  Linh
 Purpose: ETL stored procedure to load data from the [bronze] layer into the [silver] layer 
          of the Data Warehouse.

 ⚠️ IMPORTANT NOTES:
 1. This procedure uses `TRUNCATE TABLE` followed by `INSERT INTO`, meaning it fully reloads 
    each silver.* table and deletes existing data before reloading.
 2. A transaction (`BEGIN TRAN` / `COMMIT TRAN` / `ROLLBACK TRAN`) is implemented to ensure 
    atomicity: if any step fails, all changes are rolled back.
 3. Logging is currently handled using `PRINT` statements for durations and errors. For 
    production use, consider writing logs into a dedicated log table.
 4. This procedure is designed for DEV/TEST environments. Use caution when running in PROD.
 5. Must be executed after the bronze layer is fully populated.

 Last Updated: [dd/MM/yyyy]
********************************************************************************************/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	
	Declare @start_time as DATETIME, @end_time as DATETIME, @batch_start_time as DATETIME, @batch_end_time as DATETIME;

	BEGIN TRY
		BEGIN TRAN;
		SET @batch_start_time = GETDATE();
		PRINT '============================================';
		PRINT 'LOADING THE SILVER LAYER';
		PRINT '============================================';

		PRINT 'Loading CRM TABLES';
		SET @start_time = GETDATE();
		PRINT '>> Truncating silver.crm_cust_info ';
		Truncate table silver.crm_cust_info
		PRINT '>> INSERTING Data Info: silver.crm_cust_info';
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
		SET @end_time = GETDATE();
		PRINT 'silver.crm_cust_info Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating silver.crm_prd_info ';
		Truncate table silver.crm_prd_info
		PRINT '>> INSERTING Data Info: silver.crm_prd_info';
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
		SET @end_time = GETDATE();
		PRINT 'silver.crm_prd_info Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating silver.crm_sales_details ';
		Truncate table silver.crm_sales_details
		PRINT '>> INSERTING Data Info: silver.crm_sales_details';
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
		SET @end_time = GETDATE();
		PRINT 'silver.crm_sales_details Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'


		PRINT 'Loading ERP TABLES';
		SET @start_time = GETDATE();
		PRINT '>> Truncating silver.erp_cust_az12 ';
		Truncate table silver.erp_cust_az12
		PRINT '>> INSERTING Data Info: silver.erp_cust_az12';
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
		SET @end_time = GETDATE();
		PRINT 'silver.erp_cust_az12 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating silver.erp_loc_a101 ';
		Truncate table silver.erp_loc_a101
		PRINT '>> INSERTING Data Info: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		Select
		REPLACE(cid, '-', '') as cid,
		CASE WHEN TRIM(cntry) IN ('USA', 'US') THEN 'United States'
			 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END as cntry
		From bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT 'silver.erp_loc_a101 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating silver.erp_px_cat_g1v2 ';
		Truncate table silver.erp_px_cat_g1v2
		PRINT '>> INSERTING Data Info: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
		Select
		id,
		cat,
		subcat,
		maintenance
		From bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		SET @batch_end_time = GETDATE();
		PRINT 'silver.erp_px_cat_g1v2 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'
			PRINT '=========================================';
			PRINT 'Loading Silver Layer Is Completed';
			PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
            ROLLBACK TRAN;  -- 🔹 Rollback nếu lỗi
		PRINT '==================================='
		PRINT 'Error occurred while importing data in silver layer!'	
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT '===================================';
	END CATCH
END;

Exec silver.load_silver
