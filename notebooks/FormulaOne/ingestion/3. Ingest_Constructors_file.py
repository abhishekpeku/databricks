# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Ingest constructor.json file

# COMMAND ----------

constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

construct_df = spark.read.schema(constructor_schema).json("/mnt/formulastg/raw/constructors.json")

# COMMAND ----------

display(construct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop the URL column from dataframe

# COMMAND ----------

construct_drop_df = construct_df.drop(construct_df.url)

# COMMAND ----------

display(construct_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename column and Ingest Data

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

construct_final_df = construct_drop_df.withColumnRenamed("constructorId","constructor_id") \
                                      .withColumnRenamed("constructorRef","constructor_ref") \
                                      .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

#construct_final_df.write.mode('overwrite').parquet('/mnt/formulastg/processed/constructors')
construct_final_df.write.mode('overwrite').format('parquet').saveAsTable('processed.cnostructs_sql')

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/formulastg/processed/constructors

# COMMAND ----------

dbutils.notebook.exit("success")