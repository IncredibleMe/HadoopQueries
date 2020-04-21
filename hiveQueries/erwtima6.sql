DROP TABLE IF EXISTS orders_data.dim_date;

CREATE TABLE IF NOT EXISTS orders_data.dim_date (
    date_id int,
    datec string,
    yearc int,
    monthc int
)
STORED AS orc;
------------------------------
------------------------------
with min_max_dates as (
select cast('2003-01-06' as DATE) as min_date,
       cast('2006-08-23' as DATE) as max_date
)
INSERT INTO orders_data.dim_date
(date_id, `datec`, `yearc`, `monthc`)
select
row_number() over () as date_id,
datec,
year(datec) as yearc,
month(datec) as monthc
from (select
date_add(t.min_date, a.pos) as datec
from (select posexplode(split(repeat("o", datediff(max_date, t.min_date)), "o"))
from min_max_dates t
) a, min_max_dates t) d
order by d.datec;


CREATE TABLE orders_data.fact_orders (
    ORDER_DATE_KEY INT, -- Surrogate key of time dimension for ORDER_DATE
    SHIPPED_DATE_KEY INT, -- Surrogate key of time dimension for SHIPPED_DATE
    STATUS_KEY INT, -- Surrogate key of status dimension for STATUS
    PRODUCT_KEY INT, -- Surrogate key of product dimension for PRODUCT_CODE
    NUMBER_OF_ORDERS INT, -- Number of distinct orders
    TOTAL_QUANTITY_ORDERED INT, -- Total number of QUANTITY_ORDERED
    SALES_AMOUNT DECIMAL(15,2), -- Total amount paid by the customer
    MAX_UNIT_PRICE DECIMAL(15,2), -- Maximum unit price
    AVG_UNIT_PRICE DECIMAL(15,2), -- Average unit price
    MIN_UNIT_PRICE DECIMAL(15,2), -- Minimum unit price
    TOTAL_PURCHASE_AMOUNT DECIMAL(15,2) -- Total amount paid by the company
)STORED AS orc;

CREATE TABLE orders_data.dim_status (
    STATUS_KEY INT,
    STATUS STRING
)
STORED AS orc;

CREATE TABLE orders_data.dim_product (
    PRODUCT_KEY INT,
    PRODUCT_CODE STRING,
    PRODUCT_NAME STRING,
    PRODUCT_CATEGORY STRING,
    PRODUCT_VENDOR STRING
)

CREATE TEMPORARY TABLE temporder AS 
SELECT DISTINCT T1.ORDER_NUMBER,T2.ORDER_DATE,T2.SHIPPED_DATE,
T2.STATUS,T1.PRODUCT_CODE,T1.QUANTITY_ORDERED,T1.UNIT_PRICE,T1.BUY_PRICE 
FROM ORDERS_DATA.ORDERS T2
INNER JOIN orders_data.stg_order_details T1
ON T2.ORDER_NUMBER=T1.ORDER_NUMBER;

--NUMBER_OF_ORDERS=count(*)
--TOTAL_QUANTITY_ORDERED = SUM(QUANTITY_ORDERED)
--SALES_AMOUNT = QUANTITY_ORDERED*UNIT_PRICE
--MAX_UNIT_PRICE = MAX(UNIT_PRICE)
--AVG_UNIT_PRICE = AVG(UNIT_PRICE)
--MIN_UNIT_PRICE = MIN(UNIT_PRICE)
--TOTAL_PURCHASE_AMOUNT = QUANTITY_ORDERED*BUY_PRICE

WITH 
a AS (
    SELECT date_id,t2.product_code 
    FROM orders_data.dim_date t1
    INNER JOIN temporder t2
    ON t2.order_date=t1.datec),
b AS (    
    SELECT date_id,t2.product_code 
    FROM orders_data.dim_date t1
    INNER JOIN temporder t2
    ON t2.shipped_date=t1.datec), 
c AS (
    SELECT t1.status_key,t1.status,t2.product_code
    FROM orders_data.dim_status t1
    INNER JOIN temporder t2 
    ON t2.status=t1.status),
d AS (
    SELECT product_key,t2.product_code
    FROM orders_data.dim_product t1
    INNER JOIN temporder t2 
    ON t1.product_code = t2.product_code),
e AS (
    SELECT sum(quantity_ordered) as summ,t2.product_code 
    FROM temporder t2
    GROUP BY t2.product_code),
f AS (
    SELECT avg(unit_price) as avg1,t2.product_code 
    FROM temporder t2
    GROUP BY t2.product_code),
g AS (
    SELECT avg(unit_price) as avg2,t2.product_code 
    FROM temporder t2
    GROUP BY t2.product_code),
h AS (
    SELECT min(unit_price) as minn,t2.product_code 
    FROM temporder t2
    GROUP BY t2.product_code),
i AS (
    SELECT count(*) as cnt, t2.product_code
    FROM temporder t2
    GROUP BY t2.product_code)
INSERT INTO orders_data.fact_orders
SELECT a.date_id, b.date_id,c.status_key,d.product_key,
i.cnt,e.summ,unit_price*quantity_ordered,f.avg1,g.avg2,
h.minn,quantity_ordered*buy_price
from a, b,c, d, e, f,i, g, h, orders_data.dim_product, temporder
where a.product_code=b.product_code and b.product_code=c.product_code 
and c.product_code=d.product_code and d.product_code=i.product_code 
and e.product_code=h.product_code and h.product_code=g.product_code 
and g.product_code=f.product_code and temporder.product_code=a.product_code;