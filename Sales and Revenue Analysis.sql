/*******************************************************************
Add new product sequence column and amount
********************************************************************/
create table order_items as
select a.*
	, row_number() over (partition by "OrderID" order by "ProductID") as "ProductSeq"
	, "UnitPrice" * "Quantity" as "Amount" 
from order_details a;

select * from order_items;


/*******************************************************************
 Daily order count, total order quantity, product ID
********************************************************************/
select date_trunc('day', "OrderDate")::date as day
	, b."ProductID"
	, sum("Amount") as total, count(distinct a."OrderID") as daily_order_count
from orders a
	join order_items b on a."OrderID" = b."OrderID"
group by date_trunc('day', "OrderDate")::date, b."ProductID"
order by 1, 2;


/********************************************************************
Monthly sales and order quantity by product category, and the ratio to total monthly sales
********************************************************************/
with 
temp_01 as (
select d."CategoryName", date_trunc('month', "OrderDate") as month_day
	, round(sum("Amount"), 2) as total_amount, count(distinct a."OrderID") as monthly_ord_cnt
from orders a
	join order_items b on a."OrderID" = b."OrderID"
	join products c on b."ProductID" = c."ProductID" 
    join categories d on c."CategoryID" = d."CategoryID"
group by d."CategoryName", date_trunc('month', "OrderDate")
)
select *
	, round(sum(total_amount) over (partition by month_day), 2) as month_tot_amount
	, round(total_amount / sum(total_amount) over (partition by month_day), 2) as month_ratio
from temp_01;


/********************************************************************
Total sales by product and the ratio to total category sales, 
ranking within the corresponding product category
********************************************************************/
with
temp_01 as ( 
	select a."ProductID", max("ProductName") as product_name, max("CategoryName") as category_name
		, round(sum("Amount"), 2) as total_amount
	from order_items a
		join products b
			on a."ProductID" = b."ProductID"
		join categories c 
			on b."CategoryID" = c."CategoryID"
	group by a."ProductID"
)
select product_name, total_amount as product_sales
	, category_name
	, sum(total_amount) over (partition by category_name) as category_sales
	, total_amount / sum(total_amount) over (partition by category_name) as product_category_ratio
	, row_number() over (partition by category_name order by total_amount desc) as product_number
from temp_01
order by category_name, product_sales desc;


/********************************************************************
Monthly cumulative sales and same-quarter monthly cumulative sales
********************************************************************/with 
temp_01 as (
select date_trunc('month', "OrderDate")::date as month_day
	, sum("Amount") as total_amount
from orders a
	join order_items b on a."OrderID" = b."OrderID"
group by date_trunc('month', "OrderDate")::date
)
select month_day, total_amount
	, round(sum(total_amount) over (partition by date_trunc('year', month_day) order by month_day)) as cum_year_amount
	, round(sum(total_amount) over (partition by date_trunc('quarter', month_day) order by month_day)) as cum_quarter_amount
from temp_01;


/********************************************************************
5 days moving average 
*********************************************************************/
with 
temp_01 as (
select date_trunc('day', "OrderDate")::date as d_day
	, sum("Amount") as total_amount
from orders a
	join order_items b on a."OrderID" = b."OrderID"
where "OrderDate" >= to_date('1996-07-08', 'yyyy-mm-dd')
group by date_trunc('day', "OrderDate")::date
),
temp_02 as (
select d_day, total_amount
	, round(avg(total_amount) over (order by d_day rows between 4 preceding and current row)) as moving_avg_5days
	, row_number() over (order by d_day) as rnum
from temp_01
)
select d_day
	, round(total_amount)
	, rnum
	, case when rnum < 5 then Null
	       else moving_avg_5days end as moving_avg_5days
from temp_02;


/********************************************************************
Comparison of sales for the same month compared to the previous year, 
extracting the difference/ratio/growth rate of sales compared to the same month of the previous year
*********************************************************************/
with 
temp_01 as (
select date_trunc('month', "OrderDate")::date as month_day
	, sum("Amount") as total_amount
from orders a
	join order_items b on a."OrderID" = b."OrderID"
group by date_trunc('month', "OrderDate")::date
),
temp_02 as (
select month_day, round(total_amount) as curr_amount 
	, lag(month_day, 12) over (order by month_day) as prev_yr_month
	, round(lag(total_amount, 12) over (order by month_day)) as prev_yr_amount
from temp_01
) 
select *
	, round(curr_amount - prev_yr_amount) as diff_amount
	, round(100.0 * curr_amount / prev_yr_amount) as prev_pct
	, round(100.0 * (curr_amount - prev_yr_amount) / prev_yr_amount) as prev_growth_pct
from temp_02 
where prev_yr_month is not null;


/********************************************************************
Sales ratio trends compared to specific months by category criteria
*********************************************************************/
with 
temp_01 as (
select d."CategoryName" as category_name
	, to_char(date_trunc('month', "OrderDate"), 'yyyymm') as month_day
	, sum("Amount") as total_amount
from orders a
	join order_items b on a."OrderID" = b."OrderID"
	join products c on b."ProductID" = c."ProductID" 
    join categories d on c."CategoryID" = d."CategoryID"
where "OrderDate" between to_date('1996-07-01', 'yyyy-mm-dd') and to_date('1997-06-30', 'yyyy-mm-dd')
group by d."CategoryName", to_char(date_trunc('month', "OrderDate"), 'yyyymm')
)
select category_name, month_day, round(total_amount) as total_amount
	, round(first_value(total_amount) over (partition by category_name order by month_day)) as base_amount 
	, round(100.0 * total_amount/first_value(total_amount) over (partition by category_name order by month_day), 2) as base_ratio 
from temp_01;


/********************************************************************
Sales ratio trends compared to specific months by category criteria
*********************************************************************/
with 
temp_01 as (
select d."CategoryName" as category_name
	, to_char(date_trunc('month', "OrderDate"), 'yyyymm') as month_day
	, sum("Amount") as total_amount	
from orders a
	join order_items b on a."OrderID" = b."OrderID"
	join products c on b."ProductID" = c."ProductID" 
    join categories d on c."CategoryID" = d."CategoryID"
where "OrderDate" between to_date('1996-07-01', 'yyyy-mm-dd') and to_date('1997-06-30', 'yyyy-mm-dd')
group by d."CategoryName", to_char(date_trunc('month', "OrderDate"), 'yyyymm')
),
temp_02 as (
select *
	, round(total_amount) as curr_amount 
	, lag(month_day, 1) over (order by month_day) as prev_month
	, round(lag(total_amount, 1) over (PARTITION BY category_name order by month_day)) as prev_month_amount
from temp_01
) 
select category_name
	, month_day
	, curr_amount
	, prev_month_amount
	, round(curr_amount - prev_month_amount) as diff_amount	
	, round(100.0 * (curr_amount - prev_month_amount) / prev_month_amount) as prev_growth_pct
from temp_02;


/********************************************************************
Z chart for yearly sale
*********************************************************************/
with 
temp_01 as (
	select to_char(a."OrderDate", 'yyyymm') as year_month
		, sum(b."Amount") as total_amount
	from orders a
		join order_items b
			on a."OrderID" = b."OrderID"
	group by to_char(a."OrderDate", 'yyyymm')
), 
temp_02 as (
select year_month, substring(year_month, 1, 4) as year
	, round(total_amount) as monthly_total
	, round(sum(total_amount) over (partition by substring(year_month, 1, 4) order by year_month)) as cumulative_total
	, round(sum(total_amount) over (order by year_month rows between 11 preceding and current row)) as moving_annual_total
from temp_01 
)
select * from temp_02
where year = '1997';



