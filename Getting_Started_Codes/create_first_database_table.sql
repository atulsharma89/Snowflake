CREATE DATABASE OUR_FIRST_DATABASE;

CREATE TABLE OUR_FIRST_TABLE (
  first_name STRING ,
  last_name STRING ,
  address string ,
  city string ,
  state string
  );

  
--sample data for the table above is present in AWS S3 at
https://s3.ap-southeast-2.amazonaws.com/snowflake-essentials/our_first_table_data.csv

create or replace stage my_s3_stage url='s3://snowflake-essentials/';


copy into OUR_FIRST_TABLE
  from s3://snowflake-essentials/our_first_table_data.csv
  file_format = (type = csv field_delimiter = '|' skip_header = 1);

SELECT * FROM OUR_FIRST_TABLE;

SELECT COUNT(*) FROM OUR_FIRST_TABLE;



#############
assignment
##############

1 - Create a new table called customer.

The columns for this table along with the data types are as follows

  ID STRING

  Name STRING

  Address STRING

  City STRING

  PostCode Number

  State STRING

  Company STRING

  Contact STRING


CREATE TABLE customer (
  ID STRING ,
  Name STRING ,
  Address STRING ,
  City STRING ,
  PostCode Number,
  State STRING,
  Company STRING,
  Contact STRING
  );




The data for this assignment is at https://s3.ap-southeast-2.amazonaws.com/snowflake-essentials/customer.csv

The data is in CSV format and is delimited by the pipe character. The first line of the data contains the column names.



2. Create a new stage which points to the S3 bucket.

create or replace stage my_s3_stage_1 url='s3://snowflake-essentials/';



3. Load customer.csv into the customer table


copy into customer
  from s3://snowflake-essentials/customer.csv
  file_format = (type = csv field_delimiter = '|' skip_header = 1);




4. Count the number of loaded rows.



Questions for this assignment

How many rows are loaded?