--task 1
select distinct(country_code) as country ,count(country_code) as total_no_stores from dim_store_details
group by country_code

--task2
select distinct(locality) as locality, count(locality) as total_no_stores 
from dim_store_details
group by locality
order by count(locality) DESC

--task3
select dim_date_times.month, sum(orders_table.product_quantity*dim_products.product_price) as total_sales_month
from orders_table 
join dim_date_times on dim_date_times.date_uuid = orders_table.date_uuid
join dim_products on orders_table.product_code = dim_products.product_code
group by dim_date_times.month
order by total_sales_month DESC

--task4
with cte as (
	select dim_store_details.store_type as store_type, 
	sum(orders_table.product_quantity) as product_quantity_count,
	count(date_uuid) as number_sales
	from orders_table
	join dim_store_details on dim_store_details.store_code = orders_table.store_code
	group by store_type )

SELECT
    CASE
        WHEN cte.store_type = 'Web Portal' THEN 'Web Portal'
        ELSE 'Local'
    END AS location,
    SUM(cte.product_quantity_count) AS total_product_quantity,
    SUM(cte.number_sales) AS total_number_sales
FROM
    cte
GROUP BY
    CASE
        WHEN cte.store_type = 'Web Portal' THEN 'Web Portal'
        ELSE 'Local'
    END;

--task5
select dim_store_details.store_type as store_type,
sum(dim_products.product_price * orders_table.product_quantity) as total_sales,
sum(dim_products.product_price * orders_table.product_quantity) / sum(sum(dim_products.product_price * orders_table.product_quantity)) OVER () AS percentage
from orders_table
join dim_products on orders_table.product_code = dim_products.product_code
join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by store_type

--task6
select sum(dim_products.product_price * orders_table.product_quantity) as total_sales,
dim_date_times.year,
dim_date_times.month
from orders_Table
join dim_products on orders_table.product_code = dim_products.product_code
join dim_date_times on dim_date_times.date_uuid = orders_table.date_uuid
group by month, year
order by total_sales DESC

--task7
select sum(dim_store_details.staff_numbers) as total_staff_numbers,
dim_store_details.country_code
from dim_store_details
group by country_code

--task8
select dim_store_details.store_type as store_type,
sum(dim_products.product_price * orders_table.product_quantity) as total_sales,
dim_store_details.country_code as country_code
from orders_table
join dim_products on orders_table.product_code = dim_products.product_code
join dim_store_details on orders_table.store_code = dim_store_details.store_code
group by store_type, country_code
having country_code = 'DE'

--task9
--create a column to store the converted time
ALTER TABLE dim_date_times
ADD COLUMN time_column TIME;
--convert timestamp column to time type from text
UPDATE dim_date_times
SET time_column = TO_TIMESTAMP(timestamp, 'HH24:MI:SS')::TIME;

-- Create a temporary table with the next sale time for each sale
CREATE TEMPORARY TABLE tem_sales AS
SELECT 
    TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || time_column, 'YYYY-MM-DD HH24:MI:SS') AS full_timestamp,
    EXTRACT(YEAR FROM TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || time_column, 'YYYY-MM-DD HH24:MI:SS')) AS year,
    lead(TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || time_column, 'YYYY-MM-DD HH24:MI:SS')) OVER (PARTITION BY year ORDER BY TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || time_column, 'YYYY-MM-DD HH24:MI:SS')) AS next_sale_time
FROM dim_date_times;

-- Calculate the average time difference between sales for each year. epoch is used to transfer the time to seconds instead of interval value (year stuff), which can be then used for computation
WITH avg_time_diff_table AS (
    SELECT year,
           AVG(EXTRACT(EPOCH FROM (next_sale_time - full_timestamp))) AS avg_time_diff_seconds
    FROM tem_sales
    GROUP BY year
    ORDER BY year
)
--transfer numeric type to interval type and extract the integer value from the interval value
SELECT year,
       EXTRACT(HOUR FROM avg_time_diff_seconds * INTERVAL '1 second')::INT AS hours,
       EXTRACT(MINUTE FROM avg_time_diff_seconds * INTERVAL '1 second')::INT AS minutes,
       EXTRACT(SECOND FROM avg_time_diff_seconds * INTERVAL '1 second')::INT AS seconds
FROM avg_time_diff_table;
