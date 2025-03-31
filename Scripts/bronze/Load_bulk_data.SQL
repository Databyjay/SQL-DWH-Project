/* in this step we are inserting bulk data into respective tables 
we are aslo checking the duration to inserting the data 

*/

exec bronze.load_bronze;

create or alter procedure bronze.load_bronze as
begin
	declare @start_time DATETIME, @end_time DATETIME;
 	begin try
		print  '==========================================================';
		print	'Loading Bronze Layer';
		print	'==========================================================';

		print  '----------------------------------------------------------';
		print	'Loading CRM Tables';
		print	'----------------------------------------------------------';

		set @start_time = getdate();
		print	'>>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print	'>>> Inserting data into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'D:\DATA SCIENCE\MY SQL\DATA WITH BARAA_ADV- SQL\portfolio project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading duration :' + cast(datediff(second , @start_time, @end_time) as nvarchar) + 'Seconds';
		print '----------------------------------------------'
		
		set @start_time = getdate();
		print	'>>> Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		print	'>>> Inserting data into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'D:\DATA SCIENCE\MY SQL\DATA WITH BARAA_ADV- SQL\portfolio project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading duration :' + cast(datediff(second , @start_time, @end_time) as nvarchar) + 'Seconds';
		print '----------------------------------------------'
	
		set @start_time = getdate();
		print	'>>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		print	'>>> Inserting data into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'D:\DATA SCIENCE\MY SQL\DATA WITH BARAA_ADV- SQL\portfolio project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading duration :' + cast(datediff(second , @start_time, @end_time) as nvarchar) + 'Seconds';
		print '----------------------------------------------'


		
		print  '----------------------------------------------------------';
		print	'Loading ERP Tables';
		print	'----------------------------------------------------------';

		set @start_time = getdate();
		print	'>>> Truncating Table: bronze.erp_CUST_AZ12';
		truncate table bronze.erp_CUST_AZ12;
	
		print	'>>> Inserting data into: bronze.erp_CUST_AZ12';
		bulk insert bronze.erp_CUST_AZ12
		from 'D:\DATA SCIENCE\MY SQL\DATA WITH BARAA_ADV- SQL\portfolio project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading duration :' + cast(datediff(second , @start_time, @end_time) as nvarchar) + 'Seconds';
		print '----------------------------------------------'
	
		
		set @start_time = getdate();
		print	'>>> Truncating Table: bronze.erp_LOC_A101';
		truncate table bronze.erp_LOC_A101;

		print	'>>> Inserting data into: bronze.erp_LOC_A101';
		bulk insert bronze.erp_LOC_A101
		from 'D:\DATA SCIENCE\MY SQL\DATA WITH BARAA_ADV- SQL\portfolio project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading duration :' + cast(datediff(second , @start_time, @end_time) as nvarchar) + 'Seconds';
		print '----------------------------------------------'
	
		set @start_time = getdate();
		print	'>>> Truncating Table: bronze.erp_PX_CAT_G1V2';
		truncate table bronze.erp_PX_CAT_G1V2;

		print	'>>> Inserting data into: bronze.erp_PX_CAT_G1V2';
		bulk insert bronze.erp_PX_CAT_G1V2
		from 'D:\DATA SCIENCE\MY SQL\DATA WITH BARAA_ADV- SQL\portfolio project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> Loading duration :' + cast(datediff(second , @start_time, @end_time) as nvarchar) + 'Seconds';
		print '----------------------------------------------'

	end try
	begin catch
	print	'==========================================================';
	print	'Error occured during Loading Bronze Layer';
	print	'Error message' + error_message();
	print	'Error message' + cast(error_number() as nvarchar);
	print	'Error message' + cast(error_state() as nvarchar);
	print	'==========================================================';

	end catch
END
