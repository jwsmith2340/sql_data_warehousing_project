CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN 

    PRINT '-------------------------------------------------';
    PRINT 'Beginning the insert into the crm_cust_info table';
    PRINT '-------------------------------------------------';

    DECLARE @total_start_time AS DATETIME, @total_end_time AS DATETIME;
    DECLARE @start_time AS DATETIME, @end_time AS DATETIME;

    SET @total_start_time = GETDATE()
    --------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_cust_info;

    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\13042\Desktop\code\sql_data_warehousing_project\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> The load of crm_cust_info lasted: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
    --------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_prd_info;

    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\13042\Desktop\code\sql_data_warehousing_project\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> The load of crm_prd_info lasted: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
    --------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_sales_details;

    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\13042\Desktop\code\sql_data_warehousing_project\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> The load of crm_sales_details lasted: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
    --------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.erp_cust_az12;

    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\13042\Desktop\code\sql_data_warehousing_project\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> The load of erp_cust_az12 lasted: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
    --------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.erp_loc_a101;

    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\13042\Desktop\code\sql_data_warehousing_project\datasets\source_erp\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> The load of erp_loc_a101 lasted: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'
    --------------------------------------
    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\13042\Desktop\code\sql_data_warehousing_project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT '>> The load of erp_px_cat_g1v2 lasted: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' seconds'

    SET @total_end_time = GETDATE();
    PRINT '>> The TOTAL load time of all tables lasted: ' + CAST(DATEDIFF(second, @total_start_time, @total_end_time) AS VARCHAR) + ' seconds'

 END