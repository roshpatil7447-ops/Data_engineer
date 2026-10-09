select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER limit 10;

select distinct C_MKTSEGMENT from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

select * from orders limit 10;

select o_orderkey,count(*) from orders
group by o_orderkey having count(*)>1;


create or replace database sample_db;
create or replace schema sample_sch;

select * from customer_data;