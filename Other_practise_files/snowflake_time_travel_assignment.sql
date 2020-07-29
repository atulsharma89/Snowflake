Use the assignment setup steps below to create & load the required table. 
Once your table is created and loaded perform the following steps

Note the current timestamp

Update the customer table and set the Customer_ID column to be NULL. 
We are trying to emulate an accidental update.

Note the query Id of the update statement

Once you have done these steps try to recover the data before the update.

*hint

There are 3 ways to recover the data using time travel

Use a before timestamp syntax

Use an offset syntax

Use the query id



Assignment Setup steps

CREATE DATABASE time_travel;

create or replace stage sample_data_stage url='s3://snowflake-essentials-timetravel-lab';

USE DATABASE time_travel;



CREATE TABLE customer(

Customer_ID String,

Customer_Name String

);

COPY INTO customer

  FROM @sample_data_stage

  file_format = (type = csv field_delimiter = '|' skip_header = 1);


############


ALTER SESSION SET TIMEZONE = 'UTC';


SELECT CURRENT_TIMESTAMP;

2020-06-25 02:11:18.357 +0000



update CUSTOMER set Customer_ID = NULL;


SELECT * FROM CUSTOMER;


select * from customer before(timestamp => '2020-06-25 02:11:18.357'::timestamp);


-- update the Job column setting all rows to the same value

update CUSTOMER set Customer_ID = NULL;

SELECT * FROM CUSTOMER;

-- time travel to a time just before the update was run

-- time travel to 10 minutes ago (i.e. before we ran the update)
select * from CUSTOMER AT(offset => -60*10);

-- note down the query id of this query as we will use it in the time travel query as well
update CUSTOMER set Job = NULL;
--018c6f1f-00fd-b06c-0000-00000e99c991

-- time travel to the time before the update query was run
select * from CUSTOMER before(statement => '018c6f1f-00fd-b06c-0000-00000e99c991');

#########


Questions for this assignment

Were you able to see the data before the update using the before (timestamp=> syntax?

YES

Were you able to see the data before the update using the before (offset=> syntax?

YES


Were you able to see the data before the update using the before (statement=> syntax?

YES

Are there any other ways that come to your mind through which you can recover the 
data as it was before the update?

No





############
instructor solution
#############

1. Were you able to see the data before the update using the before (timestamp=> syntax?

If you weren't try setting the timezone to UTC before trying it out again

2. Were you able to see the data before the update using the before (offset=> syntax?

If you weren't then it could be that the offset value is incorrect. Trying making the offset shorter or longer depending on your scenario.

3. Were you able to see the data before the update using the before (statement=> syntax?

If you have the query id of the update statement, this is the surefire way of getting the data before the update was run.

4. Are there any other ways that come to your mind through which you can recover the data as it was before the update?

We can use time travel with cloning to clone the data before the 
update into another table.

Since we still have access to the source file, we can potentially delete all data 
from the table and reload using the COPY command. 
Of course that is not most efficient and cost effective approach in all cases.

