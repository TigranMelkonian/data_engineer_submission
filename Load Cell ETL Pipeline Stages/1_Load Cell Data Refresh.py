# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Load Cell Delta table - Daily Refresh Schedule

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F

# S3 PATH & Input File Type 
table_name = 'default.load_cell_raw'
source_data = 's3://machina-integ/load-cell/'
source_format = 'PARQUET'

# Set the data types for each column
data_types = [
  StructField("time", StringType(), True),
  StructField("value", DoubleType(), True),
  StructField("field", StringType(), True),
  StructField("robot_id", LongType(), False),
  StructField("run_uuid", DoubleType(), False),
  StructField("sensor_type", StringType(), True)
]

# Create a schema based on the data types
schema = StructType(sorted(data_types, key=lambda f: f.name))

# Read the data from the S3 folder and enforce the data types
df = spark.read.parquet(source_data, schema = schema)

# Dedupe data records
df = df.distinct()

# Drop table IF Exists
# - Truncating might be faster than dropping / overwriting completely
spark.sql("DROP TABLE IF EXISTS " + table_name)

# Create data table if the destination table does not exist
spark.sql("CREATE TABLE IF NOT EXISTS " + table_name + " (" \
  "time STRING, " + \
  "ts LONG, " + \
  "value DOUBLE, " + \
  "field STRING, " + \
  "robot_id LONG, " + \
  "run_uuid DOUBLE," +\
 "sensor_type STRING)"
)

# Write latest data to table
df.write.mode('overwrite').format('delta').saveAsTable(table_name)

