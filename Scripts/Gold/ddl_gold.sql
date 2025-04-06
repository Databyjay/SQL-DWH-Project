/* here we are creating the gold layer by studying the data from silver layer 
and joining the tables as required*/




if object id (gold.dim_customers,'v') is not null
drop gold.dim_customers;

create view gold.dim_customers as
select
	ROW_NUMBER() over(Order by cst_id) as Customer_key,
	ci.cst_id as Customer_ID,
	ci.cst_key as Customer_Number,
	ci.cst_firstname as First_name,
	ci.cst_lastname as Last_name,
	ci.cst_marital_status as Marraige_Status,
	case when ci.cst_gndr != 'n/a' then ci.cst_gndr
	else coalesce(ca.GEN, 'n/a')
	end as Gender,
	ci.cst_create_date as Created_Date,
	ca.BDATE as Birth_Date,
	la.CNTRY as Country
from Silver.crm_cust_info ci
left join Silver.erp_CUST_AZ12 ca
on	ci.cst_key = ca.CID
left join silver.erp_LOC_A101 la
on	ci.cst_key =  la.CID


if object id (gold.dim_products,'v') is not null
drop gold.dim_products;
create view gold.dim_products as
select
	ROW_NUMBER() Over(order by prd_start_dt, prd_key) as Product_Key,
	pd1.prd_id as product_ID,
	pd1.prd_key as Product_Number,
	pd1.prd_nm as Product_Name,
	pd1.Cat_ID as Category_ID,
	pd2.CAT as Category,
	pd2.SUBCAT as Sub_Category,
	pd2.MAINTENANCE as Maintenance,
	pd1.prd_cost as Product_Cost,
	pd1.prd_line as Product_Line,
	pd1.prd_start_dt as Start_Date
from Silver.crm_prd_info pd1
left join Silver.erp_PX_CAT_G1V2 pd2
on pd1.Cat_ID = pd2.ID
where prd_end_dt is null;


if object id (gold.Fact_Sales,'v') is not null
drop gold.Fact_Sales;

create view gold.Fact_Sales as
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as Order_date,
sd.sls_ship_dt as Shipping_Date,
sd.sls_due_dt as Due_Date,
sd.sls_sales as Sales_Amount,
sd.sls_quantity as Quantity,
sd.sls_price as Price
from Silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join Gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id





