# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the qualifying files

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the json files within the folder qualifying

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualify_schema = StructType(fields=[StructField("qualifyId",IntegerType(), False),
                                   StructField("raceId",IntegerType(), False),
                                   StructField("driverId",IntegerType(), False),
                                   StructField("constructorId",IntegerType(), False),
                                   StructField("number",IntegerType(), False),
                                   StructField("position",IntegerType(), True),
                                   StructField("q1",StringType(), True),
                                   StructField("q2",StringType(), True),
                                   StructField("q3",StringType(), True),])

# COMMAND ----------

qualify_df = spark.read.schema(qualify_schema).option("multiLine", True).json("/mnt/formulastg/raw/qualifying/qualifying_split*.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualify_final_df = qualify_df.withColumnRenamed("qualifyId","qualify_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database();

# COMMAND ----------

#qualify_final_df.write.mode('overwrite').parquet("/mnt/formulastg/processed/qualifying")
qualify_final_df.write.mode('overwrite').format('parquet').saveAsTable('processed.qualify_sql')

# COMMAND ----------

dbutils.notebook.exit("success")