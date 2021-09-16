# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest The pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId",IntegerType(), False),
                                     StructField("driverId",IntegerType(), False),
                                     StructField("stop",IntegerType(), False),
                                     StructField("lap",IntegerType(), False),
                                     StructField("time",StringType(), False),
                                     StructField("duration",StringType(), True),
                                     StructField("milliseconds",IntegerType(), True)])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("/mnt/formulastg/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 -  rename column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_final_df = pit_stops_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

#pit_stops_final_df.write.mode('overwrite').parquet("/mnt/formulastg/processed/pit_stops")
pit_stops_final_df.write.mode('overwrite').format('parquet').saveAsTable('processed.pit_stops_sql')

# COMMAND ----------

dbutils.notebook.exit("success")
