# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Read teh json file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                  StructField("driverRef", StringType(), True),
                                  StructField("number", IntegerType(), True),
                                  StructField("code", StringType(), True),
                                  StructField("name", name_schema),
                                  StructField("dob", DateType(), True),
                                  StructField("nationality", StringType(), True),
                                  StructField("url", StringType(), True)
                                  ])

# COMMAND ----------

drivers_df = spark.read.schema(driver_schema).json("/mnt/formulastg/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename columns and add new columns
# MAGIC ###### 1. driverId renamed t driver_id
# MAGIC ###### 2. driverRef --- driver_ref
# MAGIC ###### 3. ingestion_date added
# MAGIC ###### 4. name added with concatinating forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop unwanted columns
# MAGIC ##### 1. name.forename and name.surname
# MAGIC ##### 2. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to processed container in parquet format

# COMMAND ----------

#drivers_final_df.write.mode('overwrite').parquet("/mnt/formulastg/processed/drivers")
drivers_final_df.write.mode('overwrite').format('parquet').saveAsTable('processed.drivers_sql')

# COMMAND ----------

dbutils.notebook.exit("success")
