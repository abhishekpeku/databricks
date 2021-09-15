# Databricks notebook source
# MAGIC %run ../databases/processed_db

# COMMAND ----------

# MAGIC %sql
# MAGIC USE processed;

# COMMAND ----------

v_result = dbutils.notebook.run("1.Ingest_circuits_file",0, {"p_data_source":"Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.Ingest_Race_File",0)


# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3. Ingest_Constructors_file",0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.Ingest_Drivers_File",0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.Ingest_Resuls_File",0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.Ingest_Lap_Time_File",0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.Ingest_Pits_Stops_File",0)

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.Ingest_qualifying_files",0)

# COMMAND ----------

v_result