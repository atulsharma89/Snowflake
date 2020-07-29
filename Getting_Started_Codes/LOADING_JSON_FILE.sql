-- create a database if it doesn't already exist
CREATE DATABASE ingest_data;

USE DATABASE ingest_data;

-- create a table in which we will load the raw JSON data
CREATE TABLE organisations_json_raw (
  json_data_raw VARIANT
);
  

-- create an external stage using the S3 bucket that contains JSON data
CREATE OR REPLACE STAGE json_example_stage url='s3://snowflake-essentials/json_data';

-- list the files in the bucket
LIST @json_example_stage;

-- copy the example_json_file.json into the raw table
COPY INTO organisations_json_raw
  FROM @json_example_stage/example_json_file.json
  file_format = (type = json);
  
-- validate that the JSON has been loaded into the raw table
SELECT * FROM organisations_json_raw;
  
-- use the snowflake JSON capabilities to select value of a JSON attribute
SELECT 
	json_data_raw:data_set,
	json_data_raw:extract_date
FROM organisations_json_raw;
  
-- use flatten table function to conver the JSON data into column
SELECT
    value:name::String,
    value:state::String,
    value:org_code::String,
	json_data_raw:extract_date
FROM
    organisations_json_raw
    , lateral flatten( input => json_data_raw:organisations );
  
-- at this stage we can do a "create table as" to load the columnar data extracted from JSON
CREATE OR REPLACE TABLE organisations_ctas AS
SELECT
    VALUE:name::String AS org_name,
    VALUE:state::String AS state,
    VALUE:org_code::String AS org_code,
	json_data_raw:extract_date AS extract_date
FROM
    organisations_json_raw
    , lateral flatten( input => json_data_raw:organisations );

-- validate that the JSON data now indeed appears as proper table
SELECT * FROM organisations_ctas;

-- If you don't want to do a "create table as" you can pre-create a table
CREATE TABLE organisations (
    org_name STRING,
    state   STRING,
    org_code STRING,
	extract_date DATE
); 

-- and insert the JSON data into the table
INSERT INTO organisations 
SELECT
    VALUE:name::String AS org_name,
    VALUE:state::String AS state,
    VALUE:org_code::String AS org_code,
	json_data_raw:extract_date AS extract_date
FROM
    organisations_json_raw
    , lateral flatten( input => json_data_raw:organisations );

-- validate that the JSON data appears properly
SELECT * FROM organisations;




#############
ASSIGNMENT
#############

A JSON file containing customer data is available at the following S3 bucket.

https://snowflake-essentials-json-lab.s3-ap-southeast-2.amazonaws.com/sample_data.json



Analyze the JSON file and load the data into a snowflake table.



*hint: this file is a complete JSON document , so you will need to load the complete JSON into a variant column and then extract the information from that variant column and load into a target table.

*hint: the Snowflake stage will point to the following bucket

s3://snowflake-essentials-json-lab

Questions for this assignment

How many rows are loaded?

how many customers are located in the city with the name Cornwall?



CREATE DATABASE ingest_data;

USE DATABASE ingest_data;

CREATE TABLE organisations_json_raw (
  json_data_raw VARIANT
);

CREATE OR REPLACE STAGE json_example_stage url='s3://snowflake-essentials-json-lab/';


COPY INTO organisations_json_raw
  FROM @json_example_stage/sample_data.json
  file_format = (type = json);


SELECT * FROM organisations_json_raw;


SELECT 
	json_data_raw:table_name,
	json_data_raw:cdc_date
FROM organisations_json_raw;



-- use flatten table function to conver the JSON data into column
SELECT
    value:Customer_ID::String,
    value:Customer_Name::String,
    value:Customer_Phone::String,
    value:Customer_City::String,
	json_data_raw:cdc_date
FROM
    organisations_json_raw
    , lateral flatten( input => json_data_raw:customers );



-- at this stage we can do a "create table as" to load the columnar data extracted from JSON
CREATE OR REPLACE TABLE organisations_ctas AS
SELECT
    value:Customer_ID::String AS CUST_ID,
    value:Customer_Name::String AS CUST_NAME,
    value:Customer_Phone::String AS CUST_PHONE,
    value:Customer_City::String AS CUST_CITY,
	json_data_raw:cdc_date AS CDC_DT
FROM
    organisations_json_raw
    , lateral flatten( input => json_data_raw:customers );



 SELECT * FROM organisations_ctas

 SELECT COUNT(*) FROM organisations_ctas;
 
 39
 
 
SELECT COUNT(*) FROM organisations_ctas WHERE CUST_CITY='Cornwall';

2

