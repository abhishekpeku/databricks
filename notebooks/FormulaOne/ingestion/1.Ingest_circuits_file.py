# Databricks notebook source
# MAGIC %md
# MAGIC ##### Ingest circuits.csv

# COMMAND ----------

# MAGIC %run ../include/configuration

# COMMAND ----------

# MAGIC %run ../include/common_functions

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId", IntegerType(), False),
                                   StructField("circuitRef", StringType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("location", StringType(), True),
                                   StructField("country", StringType(), True),
                                   StructField("lat", DoubleType(), True),
                                   StructField("lng", DoubleType(), True),
                                   StructField("alt", DoubleType(), True),
                                   StructField("url", StringType(), True)])

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_folder_location}/circuits.csv")

# COMMAND ----------

circuits_select_columns_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_select_columns_df.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitute") \
.withColumnRenamed("alt","altitude") \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

circuits_final_df = curr_timestamp(circuits_renamed_df)

# COMMAND ----------

#circuits_final_df.write.mode('overwrite').parquet(f"{processed_folder_location}/circuits")
circuits_final_df.write.mode('overwrite').format("parquet").saveAsTable("processed.circuits_sql")

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc table extended processed.circuits_sql;

# COMMAND ----------

