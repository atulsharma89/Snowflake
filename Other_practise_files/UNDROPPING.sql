-- undropping a table
DROP TABLE CUSTOMER;
SELECT * FROM CUSTOMER;

UNDROP TABLE CUSTOMER;
SELECT * FROM CUSTOMER;
SELECT COUNT(*) FROM CUSTOMER;

-- undropping a schema
DROP SCHEMA PROD_CRM.PUBLIC;
SELECT * FROM CUSTOMER;

UNDROP SCHEMA PROD_CRM.PUBLIC;
SELECT * FROM CUSTOMER;
SELECT COUNT(*) FROM CUSTOMER;

-- undropping a database
DROP DATABASE PROD_CRM;
SELECT * FROM CUSTOMER;

UNDROP DATABASE PROD_CRM;
SELECT * FROM CUSTOMER;
SELECT COUNT(*) FROM CUSTOMER;