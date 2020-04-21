CREATE EXTERNAL TABLE IF NOT EXISTS orders_data.orders
(
ORDER_NUMBER STRING,
ORDER_DATE TIMESTAMP,
SHIPPED_DATE TIMESTAMP,
STATUS STRING,
COMMENTS STRING,
CUSTOMER_NUMBER STRING,
CUSTOMER_NAME STRING,
CUST_CITY STRING,
CUST_STATE STRING,
CUST_COUNTRY STRING,
CUST_COUNTRY_ISO STRING,
SALES_CURRENCY STRING,
SALES_REP_ID INT,
SALES_REP_FIRSTNAME STRING,
SALES_REP_LASTNAME STRING,
OFFICE_CODE STRING,
REPORTING_PATH ARRAY<INT>,
OFFICE_CITY STRING,
OFFICE_STATE STRING,
OFFICE_TERRITORY STRING,
OFFICE_COUNTRY STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION items terminated by ','
STORED AS TEXTFILE
LOCATION '/user/cloudera/RawData/OrdersData'
tblproperties ("skip.header.line.count"="1");
---------------------------------------------------
---------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS orders_data.orderdetails
(
ORDER_NUMBER STRING,
PRODUCT_CODE STRING,
PRODUCT_NAME STRING,
PRODUCT_CATEGORY STRING,
PRODUCT_VENDOR STRING,
QUANTITY_IN_STOCK INT,
BUY_PRICE FLOAT,
QUANTITY_ORDERED INT,
UNIT_PRICE FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/user/cloudera/RawData/OrderDetailsData'
tblproperties ("skip.header.line.count"="1");
---------------------------------------------------
---------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS orders_data.rates
(
exchangerate STRING
)
ROW FORMAT DELIMITED
LOCATION '/user/cloudera/RawData/Rates';