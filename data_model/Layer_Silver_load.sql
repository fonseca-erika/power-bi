-- Databricks notebook source
/*
Variables
1. catalog_name is the name of the catalog when UC is enabled, otherwise choose hive_metastore
2. schema_name is the name of the schema

Choose appropriate values
*/
use catalog ns_demo_db;
use schema demo_tpchstarschema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ETL process

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. dim_customer

-- COMMAND ----------

with cte as (
select 
c_custkey as customer_key,c_name as full_name,n.n_name as nationality,c.c_phone as phone,c.c_acctbal as account_balance,c.c_mktsegment as market_segment,
row_number() over(partition by c_custkey order by c_name) as Rnk
from samples.tpch.customer c 
left join 
samples.tpch.nation n 
on c.c_nationkey=n.n_nationkey
) 
MERGE INTO dim_customer t USING cte s
  ON s.customer_key=t.customer_key
WHEN NOT MATCHED THEN INSERT (customer_key,full_name,nationality,phone,account_balance,market_segment)
values (s.customer_key,s.full_name,s.nationality,s.phone,s.account_balance,s.market_segment)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. dim_part

-- COMMAND ----------

with cte as (
select 
p_partkey as part_key,p_name as full_name,p_mfgr as manufacturer,p_brand as brand,p_type as part_type,p_size as part_size,p_container as part_container,p_retailprice as retail_price,
row_number() over(partition by p_partkey order by p_name) as Rnk
from samples.tpch.part 
) 
MERGE INTO dim_part t USING cte s
  on s.part_key=t.part_key
WHEN NOT MATCHED THEN INSERT (part_key,full_name,manufacturer,brand,part_type,part_size,part_container,retail_price)
values (s.part_key,s.full_name,s.manufacturer,s.brand,s.part_type,s.part_size,s.part_container,s.retail_price)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. dim_supplier

-- COMMAND ----------

with cte as (
select 
s_suppkey as supplier_key,s_name as full_name,n.n_name as nationality,s.s_phone as phone,s.s_acctbal as account_balance,
row_number() over(partition by s_suppkey order by s_name) as Rnk
from samples.tpch.supplier s 
left join 
samples.tpch.nation n 
on s.s_nationkey=n.n_nationkey
) 
MERGE INTO dim_supplier t USING cte s
  on s.supplier_key=t.supplier_key
WHEN NOT MATCHED THEN INSERT (supplier_key,full_name,nationality,phone,account_balance)
values (s.supplier_key,s.full_name,s.nationality,s.phone,s.account_balance)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. dim_date

-- COMMAND ----------

with date_range as (
  select
  cast(date_format(s.date,"yyyyMMdd") as int) as date_sk,
  s.date,
  cast(date_format(s.date,"yyyy") as int) as year,
  cast(date_format(s.date,"MM") as int) as month,
  cast(date_format(s.date,"dd") as int) as day,
  concat(cast(date_format(s.date,"yyyy") as int),'Q',cast(date_format(s.date,"Q") as int)) as Quarter
from(
  select 
  explode(sequence(to_date('1900-01-01'), cast('2030-12-31' as date), interval 1 day)) as date
)s)
MERGE INTO dim_date t USING date_range s
  on s.`date`=t.`date`
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. fact_order_line_item

-- COMMAND ----------

with cte as (
select sd.supplier_sk,pd.part_sk,cd.customer_sk,l.l_linenumber as line_number,l.l_quantity as quantity,l.l_extendedprice as extended_price,
l.l_discount as discount,l.l_tax as tax,l.l_returnflag as return_flag,l.l_linestatus as line_status, l.l_commitdate as commit_date,
case when l.l_commitdate is null then -1 else datediff(day,l.l_commitdate,l.l_shipdate) end as ship_duration_after_commit,
case when l.l_commitdate is null then -1 else datediff(day,l.l_commitdate,l.l_receiptdate) end as receipt_duration_after_commit,o.o_orderkey as order_key,
o.o_totalprice as total_price,o.o_orderstatus as order_status,dd.date_sk as order_date_sk,o.o_orderpriority as order_priority,o.o_clerk as clerk,
l.l_shipinstruct as shipping_instructions,l.l_shipmode as shipping_mode,
row_number() over(partition by l.l_orderkey,l.l_linenumber order by l.l_commitdate) as Rnk
from samples.tpch.lineitem l
inner join samples.tpch.orders o 
on l.l_orderkey=o.o_orderkey
inner join 
dim_customer cd 
on o.o_custkey=cd.customer_key
inner join 
dim_part pd 
on l.l_partkey=pd.part_key
inner join 
dim_supplier sd 
on l.l_suppkey=sd.supplier_key
inner join 
dim_date dd 
on o.o_orderdate=dd.`date`
) 
MERGE INTO fact_order_line_item t USING cte s
  on s.order_key=t.order_key 
WHEN NOT MATCHED THEN INSERT *
