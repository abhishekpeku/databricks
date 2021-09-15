-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Create Processed DB

-- COMMAND ----------

drop database if exists processed cascade;

-- COMMAND ----------

create database if not exists processed comment 'This is processed DB' location '/mnt/formulastg/processedtables' with dbproperties (ID=007, name='Abhishek')

-- COMMAND ----------

