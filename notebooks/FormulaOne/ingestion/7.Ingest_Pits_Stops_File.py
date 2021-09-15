# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest The Lap_Times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the set od CSV files using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId",IntegerType(), False),
                                     StructField("driverId",IntegerType(), False),
                                     StructField("lap",IntegerType(), False),
                                      StructField("position",IntegerType(), True),
                                     StructField("time",StringType(), True),
                                     StructField("milliseconds",IntegerType(), True)])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/formulastg/raw/lap_times/lap_times_split*.csv")

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 -  rename column and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

#lap_times_final_df.write.mode('overwrite').parquet("/mnt/formulastg/processed/lap_times")
lap_times_final_df.write.mode('overwrite').format('parquet').saveAsTable('processed.lap_times_sql')

# COMMAND ----------

dbutils.notebook.exit("success")