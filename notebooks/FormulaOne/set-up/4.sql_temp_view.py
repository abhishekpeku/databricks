# Databricks notebook source
# MAGIC %md
# MAGIC ##### Access dataframe using SQL
# MAGIC 1. Create Temporary views on dataframes
# MAGIC 1. Access the view from SQL cell
# MAGIC 1. Access the view from python cell

# COMMAND ----------

# MAGIC %run ../include/configuration

# COMMAND ----------

# MAGIC %run ../include/common_functions

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_location}/circuits")

# COMMAND ----------

circuits_df.createOrReplaceTempView("v_circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gv_circuits where circuit_ref = 'albert_park';

# COMMAND ----------

# MAGIC %sql
# MAGIC show view in default;

# COMMAND ----------

py_select_df = spark.sql("select * from v_circuits where circuit_ref = 'albert_park'")

# COMMAND ----------

display(py_select_df)

# COMMAND ----------

circuits_df.createOrReplaceGlobalTempView("gv_circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_circuits where circuit_ref = 'albert_park';

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp;

# COMMAND ----------

