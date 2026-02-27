USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

--  Gold vs Silver Aggregation Check
SELECT 
    (SELECT SUM(sales_price) FROM SILVER.FACT_SALES) AS silver_total,
    (SELECT SUM(total_sales) FROM GOLD.SALES_BY_YEAR) AS gold_total;