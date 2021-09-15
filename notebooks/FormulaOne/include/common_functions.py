# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def curr_timestamp(input_df):
  output_df = input_df.withColumn("ingest_date", current_timestamp())
  return output_df

# COMMAND ----------

