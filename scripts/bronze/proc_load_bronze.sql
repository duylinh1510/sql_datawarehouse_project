/*
' LƯU Ý:
- Store procedure này dùng để load dữ liệu từ file CSV vào Bronze Layer.
- Các bảng trong schema [bronze] sẽ bị TRUNCATE trước khi nạp dữ liệu mới.
- Đường dẫn file CSV đang được hardcode, cần chỉnh lại khi triển khai trên máy khác.
- Yêu cầu:
    + SQL Server có quyền BULK INSERT.
    + Tài khoản SQL có quyền đọc file CSV từ thư mục chỉ định.
    + File CSV có header (dòng đầu tiên), do sử dụng FIRSTROW = 2.
- Procedure có TRY...CATCH để log thông tin lỗi (Error Number, Message, State).
'
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '============================================';
		PRINT 'LOADING THE BRONZE LAYER';
		PRINT '============================================';

		SET @start_time = GETDATE();
		PRINT 'Loading CRM TABLES';
		PRINT '>> Truncating bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT 'INSERTING Data Info: bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info
		FROM 'L:\Học Tập\Datawarehouse_Project\sql-data-warehouse-project\datasets\source_crm\data_clean_fix.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();
		PRINT '>> bronze.crm_cust_info Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT 'INSERTING Data Info: bronze.crm_prd_info ';
		BULK INSERT bronze.crm_prd_info
		FROM 'L:\Học Tập\Datawarehouse_Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();
		PRINT '>> bronze.crm_prd_info Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_sales_detail
		PRINT 'INSERTING Data Info: bronze.crm_sales_detail ';
		BULK INSERT bronze.crm_sales_detail
		FROM 'L:\Học Tập\Datawarehouse_Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();
		PRINT '>> bronze.crm_sales_detail Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'

		PRINT '--------------------------';
		PRINT 'Loading ERP TABLES';
		SET @start_time = GETDATE()
		PRINT '>> Truncating bronze.erp_cust_az12 ';
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT 'INSERTING Data Info: bronze.erp_cust_az12 ';
		BULK INSERT bronze.erp_cust_az12
		FROM 'L:\Học Tập\Datawarehouse_Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
		);
		SET @end_time=GETDATE()
		PRINT '>> bronze.erp_cust_az12 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating bronze.bronze.erp_loc_a101 ';
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT 'INSERTING Data Info: bronze.erp_loc_a101 ';
		BULK INSERT bronze.erp_loc_a101
		FROM 'L:\Học Tập\Datawarehouse_Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE()
		PRINT '>> bronze.erp_loc_a101 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating bronze.erp_px_cat_g1v2 ';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT 'INSERTING Data Info: bronze.erp_px_cat_g1v2 ';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'L:\Học Tập\Datawarehouse_Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
		);
		SET @end_time = GETDATE();
		PRINT '>> bronze.erp_px_cat_g1v2 Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>> -------------------------------';
		SET @batch_end_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Loading Bronze Layer Is Completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
	END TRY
	BEGIN CATCH
		PRINT '==================================='
		PRINT 'Error occured while importing data in bronze layer!'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '==================================='
	END CATCH
END;

EXEC bronze.load_bronze
