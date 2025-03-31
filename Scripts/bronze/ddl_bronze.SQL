/* this code snippet created the empty tables in our database 
    we are also taking care that if the table is already present in DB
    then we are dropping and creating table at same time
*/

use DWH;

go


	if OBJECT_ID('bronze.crm_cust_info', 'u') is not null
		drop table bronze.crm_cust_info;
	create table bronze.crm_cust_info(
	cst_id	int,
	cst_key	Nvarchar(50),
	cst_firstname nvarchar(50),
	cst_lastname nvarchar(50),
	cst_marital_status nvarchar(50),
	cst_gndr nvarchar(50),
	cst_create_date date
	);

	
	
	if OBJECT_ID('bronze.crm_prd_info', 'u') is not null
		drop table bronze.crm_prd_info;
	create table bronze.crm_prd_info(
	prd_id	int,
	prd_key	nvarchar(50),
	prd_nm	nvarchar(80),
	prd_cost	int,
	prd_line	varchar(10),
	prd_start_dt	date,
	prd_end_dt date
	);

	
	
	if OBJECT_ID('bronze.crm_sales_details', 'u') is not null
		drop table bronze.crm_sales_details;
	create table bronze.crm_sales_details(
	sls_ord_num	Nvarchar(50),
	sls_prd_key	Nvarchar(50),
	sls_cust_id	int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int
	);

	
	if OBJECT_ID('bronze.erp_CUST_AZ12', 'u') is not null
		drop table bronze.erp_CUST_AZ12;
	create table bronze.erp_CUST_AZ12(
	CID	nvarchar(50),
	BDATE date,
	GEN varchar(10)
	);

	
	if OBJECT_ID('bronze.erp_LOC_A101', 'u') is not null
		drop table bronze.erp_LOC_A101;
	create table bronze.erp_LOC_A101(
	CID	nvarchar(50),
	CNTRY varchar(50)
	);

	
	if OBJECT_ID('bronze.erp_PX_CAT_G1V2', 'u') is not null
		drop table bronze.erp_PX_CAT_G1V2;
	create table bronze.erp_PX_CAT_G1V2(
	ID varchar(50),
	CAT varchar(50),
	SUBCAT varchar(50),
	MAINTENANCE varchar(50),
	);
