-- Create products_sales table
CREATE TABLE IF NOT EXISTS orders_data.products_sales (
ORDER_DATE DATE ,
PRODUCT_CODE STRING,
SALES_AMOUNT DECIMAL(15,2)
)
STORED AS orc;
-- Insert sales amount per order_date and product_code into the products_sales
insert into orders_data.products_sales
select cast(o.order_date as DATE) as ORDER_DATE,
       d.PRODUCT_CODE,
       sum(d.unit_price*d.quantity_ordered) as SALES_AMOUNT
from orders_data.stg_order_details d, orders_data.stg_orders o 
where d.order_number=o.order_number
group by d.product_code, cast(o.order_date as DATE);
----------------------------------
----------------------------------
-----ERWTIMA A--------------------
SELECT order_date, product_code, sales_amount, 
lag(sales_amount,1) OVER (PARTITION BY product_code ORDER BY order_date), 
lead(sales_amount,1) OVER (PARTITION BY product_code ORDER BY order_date),
FROM orders_data.products_sales;
----------------------------------
----------------------------------
-----ERWTIMA B--------------------
WITH 
a AS (
    SELECT max(sales_amount) 
    OVER (PARTITION BY order_date ORDER BY order_date ASC) 
    AS max_sales_amount)
    FROM orders_data.products_sales)
SELECT order_date, product_code, sales_amount, a.max_sales_amount-sales_amount
max(sales_amount) OVER (PARTITION BY order_date ORDER BY product_code ASC) AS max_sales_amount
FROM orders_data.products_sales;
----------------------------------
----------------------------------
-----ERWTIMA C--------------------
WITH 
a AS (
    SELECT sum(sales_amount) 
    OVER(
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW
    ) sum_sales_amount
    FROM orders_data.products_sales  
)  
SELECT order_date, product_code, sales_amount, a.sum_sales_amount
FROM orders_data.products_sales;