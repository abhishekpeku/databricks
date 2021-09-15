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

show tables;

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

desc extended default.circuits_table_ext_py;

-- COMMAND ----------

