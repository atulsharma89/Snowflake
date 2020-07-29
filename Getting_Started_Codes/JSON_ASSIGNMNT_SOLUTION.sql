-- create a database if it doesn't already exist
CREATE DATABASE load_json_data_lab;

USE DATABASE load_json_data_lab;

-- create a table in which we will load the raw JSON data
CREATE TABLE customers_JSON_raw (
  raw_json_col VARIANT
);
  

-- create an external stage using the S3 bucket that contains JSON data
CREATE OR REPLACE STAGE json_lab_stage url='s3://snowflake-essentials-json-lab';

-- list the files in the bucket
LIST @json_lab_stage;

-- copy the JSON file present in the stage into the table
COPY INTO customers_JSON_raw
  FROM @json_lab_stage
  file_format = (type = json);
  
-- validate that the JSON has been loaded into the raw table
SELECT * FROM customers_JSON_raw;
  
-- use the snowflake JSON capabilities to start building your relational format table
SELECT 
	raw_json_col:cdc_date
FROM customers_JSON_raw;
  
-- use flatten table function to conver the JSON data into column
SELECT
	value:Customer_ID::String,
    value:Customer_Name::String,
	value:Customer_Phone::String,
    value:Customer_City::String,
	raw_json_col:cdc_date
FROM
    customers_JSON_raw
    , lateral flatten( input => raw_json_col:customers );
  

-- create table in which we will load this data
CREATE TABLE customers (
    Customer_ID STRING,
	Customer_Name STRING,
	Customer_Phone STRING,
	Customer_City STRING,
	cdc_date DATE
); 

-- and insert the JSON data into the table
INSERT INTO customers 
SELECT
	value:Customer_ID::String,
    value:Customer_Name::String,
	value:Customer_Phone::String,
    value:Customer_City::String,
	raw_json_col:cdc_date
FROM
    customers_JSON_raw
    , lateral flatten( input => raw_json_col:customers );
  

-- validate that the JSON data is loaded properly
SELECT * FROM customers;

-- select the count of rows. this should return 39 rows
SELECT COUNT(*) FROM customers;

-- select the count of customers for Cornwall city. this should return two rows
SELECT COUNT(*) FROM customers where CUSTOMER_CITY = 'Cornwall';