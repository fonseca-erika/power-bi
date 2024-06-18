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

drop table if exists dim_customer;
drop table if exists dim_supplier;
drop table if exists dim_part;
drop table if exists dim_date;
drop table if exists fact_order_line_item;

-- COMMAND ----------

create table dim_customer
(
 customer_sk bigint not null generated always as identity, 
 customer_key bigint not null,
 full_name varchar(50),
 nationality varchar(50),
 phone varchar(20),
 account_balance decimal(18,2),
 market_segment varchar(20)
);

create table dim_supplier 
(
 supplier_sk bigint not null generated always as identity, 
 supplier_key bigint not null,
 full_name varchar(50),
 nationality varchar(50),
 phone varchar(20),
 account_balance decimal(18,2)
);

create table dim_part
(
 part_sk bigint not null generated always as identity, 
 part_key bigint not null,
 full_name varchar(100),
 manufacturer varchar(20),
 brand varchar(10),
 part_type varchar(50),
 part_size int ,
 part_container varchar(20),
 retail_price decimal(18,2)
);



create  table dim_date (
date_sk bigint not null,
`date` date not null,
`year` int not null,
`month` int not null,
`day` int not null,
`quarter` string not null
);


-- COMMAND ----------

ALTER TABLE dim_customer ADD PRIMARY KEY (customer_sk) RELY;
ALTER TABLE dim_supplier ADD PRIMARY KEY (supplier_sk) RELY;
ALTER TABLE dim_part ADD PRIMARY KEY (part_sk) RELY;
ALTER TABLE dim_date ADD PRIMARY KEY (date_sk) RELY;


-- COMMAND ----------

create table fact_order_line_item
(
  order_date_sk bigint not null CONSTRAINT order_date_dim_date_fk FOREIGN KEY  REFERENCES dim_date,
  supplier_sk bigint not null CONSTRAINT dim_supplier_fk FOREIGN KEY  REFERENCES dim_supplier,
  part_sk bigint not null CONSTRAINT dim_part_fk FOREIGN KEY  REFERENCES dim_part,
  customer_sk bigint not null CONSTRAINT dim_customer_fk FOREIGN KEY  REFERENCES dim_customer,
  order_key bigint,
  line_number int,
  order_status varchar(10),
  order_priority varchar(20),
  clerk varchar(50),
  quantity int,
  total_price decimal(18,5),
  extended_price decimal(18,2),
  discount decimal(18,2),
  tax decimal(18,2),
  return_flag varchar(1),
  line_status varchar(1),
  commit_date date,
  ship_duration_after_commit int,
  receipt_duration_after_commit int,
  shipping_instructions varchar(50),
  shipping_mode varchar(20)
);
