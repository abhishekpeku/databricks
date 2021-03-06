-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Learn Objectives
-- MAGIC 1. spark sql documentation
-- MAGIC 1. create database demo
-- MAGIC 1. DATA tab in UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. find the current_database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### https://spark.apache.org/docs/latest/sql-ref.html

-- COMMAND ----------

create database if not exists demo

-- COMMAND ----------

show databases;

-- COMMAND ----------

desc database demo;

-- COMMAND ----------

desc database extended demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables in processed;

-- COMMAND ----------

-- MAGIC %run ../include/configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC circuits_df = spark.read.parquet(f"{processed_folder_location}/circuits")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC circuits_df.write.format("parquet").saveAsTable("demo.circuits_python")

-- COMMAND ----------

select * from demo.circuits_python;

-- COMMAND ----------

USE demo;
show tables;

-- COMMAND ----------

desc extended demo.circuits_python;

-- COMMAND ----------

create table demo.circuits_sql as select * from demo.circuits_python;

-- COMMAND ----------

drop table demo.circuits_sql

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

-- MAGIC 
-- MAGIC %python
-- MAGIC circuits_ext_py = circuits_df.write.format("parquet").option("path", f"{processed_folder_location}/circuits_ext_py").saveAsTable("circuits_table_ext_py")

-- COMMAND ----------

show tables;

-- COMMAND ----------

desc extended processed.circuits_sql;

-- COMMAND ----------

desc extended processed.races_sql;

-- COMMAND ----------

select t2.* from processed.circuits_sql t1, processed.races_sql t2 where t1.circuit_id = t2.circuit_id;

-- COMMAND ----------

select split(t1.name,' ') from processed.circuits_sql t1;

-- COMMAND ----------

desc table extended processed.circuits_sql

-- COMMAND ----------

select nationality, name, dob, rank() over(partition by (nationality) order by dob desc) as age_rank from processed.drivers_sql order by nationality, age_rank;

-- COMMAND ----------


