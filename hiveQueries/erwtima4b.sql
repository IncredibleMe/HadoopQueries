
CREATE EXTERNAL TABLE IF NOT EXISTS orders_data.rates
(
exchangerate STRING
)
ROW FORMAT DELIMITED
LOCATION '/user/cloudera/RawData/Rates';
-------------------------------------
-------------------------------------
CREATE TABLE IF NOT EXISTS orders_data.stg_conv_rates (
    INDATE DATE,
    RATES STRING
)
STORED AS orc;
-------------------------------------
-------------------------------------
INSERT INTO orders_data.stg_conv_rates
SELECT cast(get_json_object(exchangerate, '$.date') as date),cast(get_json_object(exchangerate, '$.rates') as string)
FROM orders_data.rates;

INSERT INTO orders_data.stg_conv_rates
SELECT to_date(from_unixtime(unix_timestamp(get_json_object(exchangerate, '$.date'),'yyyy-MM-dd'))),get_json_object(exchangerate, '$.rates')
FROM orders_data.rates;
