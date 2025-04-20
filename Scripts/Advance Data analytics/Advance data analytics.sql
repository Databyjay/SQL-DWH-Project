use DataWarehouseAnalytics

-- analyze the sales performance over time
---(Change over time)

select
year(order_date) by_year,
month(order_date) by_month,
sum(sales_amount) as total_sales,
count(distinct customer_key) total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where month(order_date) is not null
group by year(order_date),month(order_date)
order by year(order_date),month(order_date);

------ or can use this code

select
format(order_date, 'yyyy-MMM') as date_month,
sum(sales_amount) as total_sales,
count(distinct customer_key) total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where format(order_date, 'yyyy-MMM') is not null
group by format(order_date, 'yyyy-MMM')
order by format(order_date, 'yyyy-MMM');


----- Cumulative Analysis
--- calculate the total sales per each month & running total of sales over time

select
sum(sales_amount) as total_sales, --- can also use window functions
sum(quantity) as total_quantity,
datetrunc(month, order_date) by_month
from gold.fact_sales
where datetrunc(month, order_date) is not null
group by datetrunc(month, order_date);

-- by using window function

select
by_month as order_date,
total_sales,
sum(total_sales) over(order by by_month) as running_total_revenue,
avg(moving_average) over(order by by_month) as running_total_revenue
from
(
	select
	sum(sales_amount) as total_sales,
	avg(sales_amount) as moving_average,
	datetrunc(YEAR, order_date) by_month
	from gold.fact_sales
	where datetrunc(YEAR, order_date) is not null
	group by datetrunc(YEAR, order_date)
)t


---- performance analysis
-- Analyze the yearly performance of products by comparing each product sale to both its average sale performance 
--- & previous year sale
with yearly_performance_sales as 
(
	select
	year(f.order_date) as Order_year,
	p.product_name as product_name,
	sum(f.sales_amount) as Current_Sales
	from gold.fact_sales f
	left join gold.dim_products p
	on f.product_key = p.product_key 
	where year(f.order_date) is not null
	group by year(f.order_date), p.product_name
)
select 
Order_year,
product_name,
Current_Sales,
avg(Current_Sales) over(partition by product_name) as AVG_Sales_product,
Current_Sales - avg(Current_Sales) over(partition by product_name) as avg_diff,
case 
	when Current_Sales - avg(Current_Sales) over(partition by product_name) > 0 then 'Above Average'
	when Current_Sales - avg(Current_Sales) over(partition by product_name) < 0 then 'Below Average'
	else 'Average'
end Average_change,
lag(Current_Sales) over(partition by product_name order by Order_year) prev_sales,
Current_Sales - lag(Current_Sales) over(partition by product_name order by Order_year) as Py_difference,
case 
	when Current_Sales - lag(Current_Sales) over(partition by product_name order by Order_year) > 0 then 'Increased'
	when Current_Sales - lag(Current_Sales) over(partition by product_name order by Order_year) < 0 then 'Decreased'
	else 'Same'
end Sale_diff
from yearly_performance_sales
order by product_name,Order_year;

---- PART TO WHOLE
---- Analyze how a individual part is performing compared to the overall allowing us to to understand which category has greatest impact on business
with Category_analysis as
(
	select
	sum(f.sales_amount) as total_sale,
	p.category
	from gold.fact_sales f
	left join gold.dim_products p
	on f.product_key = p.product_key
	group by p.category
	--order by total_sale desc
)
select
category ,
total_sale,
sum(total_sale) over() as sale,
concat(round((cast(total_sale as float)/sum(total_sale) over())*100 , 2), '%') as percentage_sales
from Category_analysis
order by total_sale desc

--- Grouping the data into specific range
with specific_range as 
(
	select
	product_key,
	product_name,
	cost,
	case 
				when cost < 100 then 'Below 100'
				when cost between 100 and 500 then '100-500'
				when cost between 500 and 1000 then '500-1000'
			else 'above 1000' end as cost_range
	from gold.dim_products 
)
select 
cost_range,
count(product_key) as total_products
from specific_range
group by cost_range
order by total_products desc;

/* group customers into 3 segments based on their spending behaviour
---vip: atleast 12 months of history & spending more than 5000 euro
-regular: at least 12 months of history by spending less than 5000 euro
- new: less than 12 months 
find the total no of customers in each group */
with Customer_se as
(
	select 
	f.customer_key,
	sum(f.sales_amount) as sale_amount_by_customer,
	min(f.order_date) as first_order,
	max(f.order_date) as last_order,
	datediff(MONTH, min(f.order_date),max(f.order_date)) as customer_tenure,
	case
		when datediff(MONTH, min(f.order_date),max(f.order_date)) >= 12 and sum(f.sales_amount) > 5000 then 'VIP Customer'
		when datediff(MONTH, min(f.order_date),max(f.order_date)) >= 12 and sum(f.sales_amount) < 5000 then 'Regular Customer'
			else 'NEW Customer'
		end as Customer_segment
	from gold.dim_customers c
	left join gold.fact_sales f
	on c.customer_key = f.customer_key
	group by f.customer_key
	)
select 
Customer_segment,
count(Customer_segment) as customer_count
from Customer_se
group by Customer_segment
order by customer_count desc;


/* 
===============================================================================================================================
Customer report
===============================================================================================================================
purpose :- this report consolidate key customer metrics & behaviour
Highlights: 
	1: Gather essentual fields such as names,ages & transaction details
	2: Segment Customers into categories
	3: Aggregate customer level metrics
		- total orders
		- total sales
		- total quantity purchased
		- total product
		- lifespan (in months)
	4: Calculate Valuble KPI's
			recency
			avg order value
			avg monthly spend 
*/

--- Base qurey : Retrive core columns from Tables
CREATE VIEW gold.customer_report as
with base_query as
(
	select 
	f.order_number,
	f.product_key,
	f.order_date,
	f.sales_amount,
	f.quantity,
	c.customer_key,
	c.customer_number,
	CONCAT(c.first_name, ' ', c.last_name) as full_name,
	DATEDIFF(YEAR, c.birthdate, GETDATE()) as age
	from gold.fact_sales f
	left join gold.dim_customers c
	on f.customer_key = c.customer_key
	where order_date is not null
),
customer_aggregation as
(
	select 
	customer_key,
	customer_number,
	full_name,
	age,
	count(distinct order_number) as total_orders,
	sum(sales_amount) as total_sales,
	sum(quantity)  as total_quantity,
	count(distinct product_key) as total_products,
	max(order_date) as last_order_date,
	datediff(MONTH, min(order_date),max(order_date)) as life_span
	from base_query
	GROUP BY customer_key, customer_number, full_name, age
)
select 
customer_key,
customer_number,
full_name,
age,
case 
		when age < 20 then 'Below 20'
		when age between 20 and 29 then '20-29'
		when age between 30 and 39 then '30-39'
		when age between 40 and 49 then '40-49'
		else '50 and Above'
	end as age_group,
case 
		when life_span >= 12 and total_sales > 5000 then 'VIP'
		when life_span >= 12 and total_sales < 5000 then 'Regular'
		else 'New'
	end as Customer_Segment,
		
total_orders,
total_sales,
total_quantity,
total_products,
case when total_orders = 0 then 0
else (total_sales/total_orders) 
end as Avg_order_value,
last_order_date,
datediff(MONTH, last_order_date, getdate()) as Recency,
life_span,
case when life_span = 0 then total_sales
else (total_sales/life_span) 
end as Avg_monthly_spend
from customer_aggregation;


select * from gold.dim_products
go
select * from gold.fact_sales

/* building product report 
in three steps 
1) base query pulling all required columns from the tables
2) product aggregations: summarize key metrics at product level
3) final query: combines all product result into one output */

create view gold.product_report as
with base_query as
(
select 
f.order_number,
f.customer_key,
f.order_date,
f.sales_amount,
f.quantity,
p.product_key,
p.product_name,
p.category,
p.subcategory,
p.cost
from gold.fact_sales f 
left join gold.dim_products p
on f.product_key = p.product_key
where f.order_date is not null
),
product_aggregation as(
select 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	datediff(month, min(order_date), max(order_date)) as life_span,
	max(order_date) as last_order_date,
	count(distinct order_number) as total_orders,
	count(distinct customer_key) as total_customers,
	sum(sales_amount) as total_sales,
	sum(quantity) as total_quantity,
	rounD(avg(cast(sales_amount as float)/nullif(quantity,0)),1) as avg_selling_price
	from base_query
	group by 
			product_key,
			product_name,
			category,
			subcategory,
			cost
	)
select
	product_key, product_name, category, subcategory, cost,last_order_date,
	datediff(month, last_order_date, getdate()) as product_recency,
	case 
		when total_sales > 50000 then 'High Performer'
		when total_sales >= 10000 then 'Mid Range'
		else 'Low Performer'
	end as product_segment,
	life_span,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	case
		when total_orders = 0 then 0
		else total_sales/total_orders 
		end as avg_order_revenue,
	case 
		when life_span = 0 then 0
		else total_sales/life_span
	end as avg_product_revenue
	from product_aggregation


select * from gold.product_report