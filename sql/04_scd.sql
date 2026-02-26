desc table snowflake_sample_data.TPCDS_SF100TCL.CUSTOMER;

desc table snowflake_sample_data.tpcds_sf100tcl.customer_address;

use warehouse compute_wh;

use database snowflake_learning_db;

create or replace table silver.customer_dim(
       customer_sk number autoincrement,
       customer_id number,
       state string,
       start_date date,
       end_date date,
       is_current string
);

insert into silver.customer_dim
(customer_id,state,start_date,end_date,is_current)
select c.c_customer_sk,ca.ca_state,current_date,null,'Y'
from snowflake_sample_data.tpcds_sf100tcl.customer c
join snowflake_sample_data.tpcds_sf100tcl.customer_address ca
on c.c_current_addr_sk=ca.ca_address_sk;

select count(*) from silver.customer_dim;

select * from silver.customer_dim limit 5;

create or replace table SILVER.CUSTOMER_STAGE AS
SELECT customer_id, state
FROM SILVER.CUSTOMER_DIM
WHERE is_current ='Y';

UPDATE SILVER.CUSTOMER_STAGE
SET state='TX'
WHERE customer_id = 28437793;

MERGE INTO SILVER.CUSTOMER_DIM target
USING SILVER.CUSTOMER_STAGE source 
On target.customer_id=source.customer_id
AND target.is_current='Y'
WHEN MATCHED AND target.state <> source.state THEN
UPDATE 
SET target.end_date = CURRENT_DATE, 
target.is_current = 'N'
WHEN NOT MATCHED THEN
INSERT (customer_id, state, start_date, end_date, is_current)
VALUES (source.customer_id, source.state, CURRENT_DATE, NULL, 'Y');

SELECT *
FROM SILVER.CUSTOMER_DIM
WHERE customer_id = 28437793
ORDER BY start_date;