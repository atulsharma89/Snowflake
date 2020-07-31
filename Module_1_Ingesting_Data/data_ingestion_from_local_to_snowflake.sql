
##############
performing operations on snowflake
##############

Step1 - creating database:

create or replace database mydatabase;


step 2 - Creating warehouse

create or replace warehouse mywarehouse with
  warehouse_size='X-SMALL'
  auto_suspend = 120
  auto_resume = true
  initially_suspended=true;
  
  
Step 3: Create file format:

create or replace file format mycsvformat
  type = 'CSV'
  field_delimiter = '|'
  skip_header = 0;
  

step 4: Creating stage layer:

 create or replace stage my_csv_stage
  file_format = mycsvformat;
  

Step 5: Loading data from local to stage layer


ATULSHARMA89#COMPUTE_WH@MYDATABASE.PUBLIC>put file:///Users/atul595525/Desktop/TPCxBB_q26/Data_copy/item_10_1.csv @my_csv_stage auto_compress=true;
item_10_1.csv_c.gz(8.80MB): [##########] 100.00% Done (41.115s, 0.21MB/s).      
+---------------+------------------+-------------+-------------+--------------------+--------------------+----------+---------+
| source        | target           | source_size | target_size | source_compression | target_compression | status   | message |
|---------------+------------------+-------------+-------------+--------------------+--------------------+----------+---------|
| item_10_1.csv | item_10_1.csv.gz |    21514157 |     9224513 | NONE               | GZIP               | UPLOADED |         |
+---------------+------------------+-------------+-------------+--------------------+--------------------+----------+---------+

ATULSHARMA89#COMPUTE_WH@MYDATABASE.PUBLIC>put file:///Users/atul595525/Desktop/TPCxBB_q26/Data_copy/*.csv @my_csv_stage auto_compress=true;
item_10.csv_c.gz(8.80MB): [##########] 100.00% Done (20.640s, 0.43MB/s).        
store_sales_10.csv_c.gz(395.49MB): [##########] 100.00% Done (859.783s, 0.46MB/s).
+--------------------+-----------------------+-------------+-------------+--------------------+--------------------+----------+---------+
| source             | target                | source_size | target_size | source_compression | target_compression | status   | message |
|--------------------+-----------------------+-------------+-------------+--------------------+--------------------+----------+---------|
| item_10.csv        | item_10.csv.gz        |    21514157 |     9224280 | NONE               | GZIP               | UPLOADED |         |
| store_sales_10.csv | store_sales_10.csv.gz |  1177688815 |   414697380 | NONE               | GZIP               | UPLOADED |         |
+--------------------+-----------------------+-------------+-------------+--------------------+--------------------+----------+---------+
2 Row(s) produced. Time Elapsed: 994.404s



Step 6: Creating csv_storesales table:

drop table csv_storesales;

--74 milli second


create or replace temporary table csv_storesales
(
 ss_sold_date_sk String,
      ss_sold_time_sk String,
      ss_item_sk String,
      ss_customer_sk String,
      ss_cdemo_sk String,
      ss_hdemo_sk String,
      ss_addr_sk String,
      ss_store_sk String,
      ss_promo_sk String,
      ss_ticket_number String,
      ss_quantity String,
      ss_wholesale_cost String,
      ss_list_price String,
      ss_sales_price String,
      ss_ext_discount_amt String,
      ss_ext_sales_price String,
      ss_ext_wholesale_cost String,
      ss_ext_list_price String,
      ss_ext_tax String,
      ss_coupon_amt String,
      ss_net_paid String,
      ss_net_paid_inc_tax String,
      ss_net_profit String
      );
      
--189 milli second

Step 8: Loading data from stage layer to store sales tables


copy into csv_storesales
  from @my_csv_stage/store_sales_10.csv.gz
  file_format = (format_name = mycsvformat)
  on_error = 'skip_file';
  
--1 minute 35 second

drop table csv_item;

--91milli second


step 9: creating csv_item table

create or replace temporary table csv_item
(
   i_item_sk                   bigint              
  , i_item_id                 string              
  , i_rec_start_date          DATE
  , i_rec_end_date            DATE
  , i_item_desc               string
  , i_current_price           decimal(7,2)
  , i_wholesale_cost          decimal(7,2)
  , i_brand_id                NUMBER
  , i_brand                   string
  , i_class_id                NUMBER
  , i_class                   string
  , i_category_id             NUMBER
  , i_category                string
  , i_manufact_id             NUMBER
  , i_manufact                string
  , i_size                    string
  , i_formulation             string
  , i_color                   string
  , i_units                   string
  , i_container               string
  , i_manager_id              NUMBER
  , i_product_name            string
      );


--210 milli second


step 10 : loading data into csv_item table:


copy into csv_item
from @my_csv_stage/item_10_1.csv.gz
file_format = (format_name = mycsvformat)
on_error = 'skip_file';

--2.33 second


drop table tpcxbb_q26_result;

--68 milli second


step 11: creating final table:

CREATE TABLE tpcxbb_q26_result (
  cid  BIGINT,
  id1  double PRECISION,
  id2  double PRECISION,
  id3  double PRECISION,
  id4  double PRECISION,
  id5  double PRECISION,
  id6  double PRECISION,
  id7  double PRECISION ,
  id8  double PRECISION,
  id9  double PRECISION,
  id10 double PRECISION,
  id11 double PRECISION,
  id12 double PRECISION,
  id13 double PRECISION,
  id14 double PRECISION,
  id15 double PRECISION
);

--190 milli second


step 12: Insert data into final table from temp tables

INSERT INTO tpcxbb_q26_result
SELECT a.ss_customer_sk AS cid,
  count(CASE WHEN i.i_class_id=1  THEN 1 ELSE NULL END) AS id1,
  count(CASE WHEN i.i_class_id=2  THEN 1 ELSE NULL END) AS id2,
  count(CASE WHEN i.i_class_id=3  THEN 1 ELSE NULL END) AS id3,
  count(CASE WHEN i.i_class_id=4  THEN 1 ELSE NULL END) AS id4,
  count(CASE WHEN i.i_class_id=5  THEN 1 ELSE NULL END) AS id5,
  count(CASE WHEN i.i_class_id=6  THEN 1 ELSE NULL END) AS id6,
  count(CASE WHEN i.i_class_id=7  THEN 1 ELSE NULL END) AS id7,
  count(CASE WHEN i.i_class_id=8  THEN 1 ELSE NULL END) AS id8,
  count(CASE WHEN i.i_class_id=9  THEN 1 ELSE NULL END) AS id9,
  count(CASE WHEN i.i_class_id=10 THEN 1 ELSE NULL END) AS id10,
  count(CASE WHEN i.i_class_id=11 THEN 1 ELSE NULL END) AS id11,
  count(CASE WHEN i.i_class_id=12 THEN 1 ELSE NULL END) AS id12,
  count(CASE WHEN i.i_class_id=13 THEN 1 ELSE NULL END) AS id13,
  count(CASE WHEN i.i_class_id=14 THEN 1 ELSE NULL END) AS id14,
  count(CASE WHEN i.i_class_id=15 THEN 1 ELSE NULL END) AS id15
FROM csv_storesales  a
INNER JOIN csv_item i 
  ON (a.ss_item_sk = i.i_item_sk
  AND i.i_category IN ('Books')
  AND a.ss_customer_sk IS NOT NULL
)
GROUP BY a.ss_customer_sk
HAVING count(a.ss_item_sk) > 5
;

--998 milli seconds

step 13: aggregating results:


select sum(id1),
sum(id2),
sum(id3),
sum(id4),
sum(id5),
sum(id6),
sum(id7),
sum(id8),
sum(id9),
sum(id10),
sum(id11),
sum(id12),
sum(id13),
sum(id14),
sum(id15)
from tpcxbb_q26_result;


--229 milli second


select 
(
sum(id1)+
sum(id2)+
sum(id3)+
sum(id4)+
sum(id5)+
sum(id6)+
sum(id7)+
sum(id8)+
sum(id9)+
sum(id10)+
sum(id11)+
sum(id12)+
sum(id13)+
sum(id14)+
sum(id15)
) as total_sum
from tpcxbb_q26_result;

--379 milli second
