# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Read CSV Race.csv using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("dbfs:/mnt/formulastg/raw/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Add ingestion date and race timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

# COMMAND ----------

races_withtimestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                 .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_withtimestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Select only the required columns and rename them as required

# COMMAND ----------

races_selected_df = races_withtimestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'), col('circuitId').alias('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write data to parquet format

# COMMAND ----------

#races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formulastg/processed/races')
races_selected_df.write.mode('overwrite').partitionBy('race_year').format('parquet').saveAsTable('processed.races_sql')

# COMMAND ----------

display(spark.read.parquet('/mnt/formulastg/processed/races'))

# COMMAND ----------

dbutils.notebook.exit("success")