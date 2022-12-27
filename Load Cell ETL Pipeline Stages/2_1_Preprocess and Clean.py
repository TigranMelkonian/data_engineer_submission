# Databricks notebook source
# Imports
import pyspark.sql.functions as F
from pyspark.sql import Window
import sys

# Source data from loacal data store
table_name_source = 'default.load_cell_raw'
table_name_result = 'default.load_cell_clean'

df_raw = spark.sql("SELECT * FROM " + table_name_source)
display(df_raw) # Check output

# COMMAND ----------

# Ensure headers are lower case
df_raw = df_raw.select([F.col(col).alias(col.lower()) for col in df_raw.columns])
    
# Check Dtypes
df_raw.dtypes == [('time', 'string'),
                 ('ts', 'bigint'),
                 ('value', 'double'),
                 ('field', 'string'),
                 ('robot_id', 'bigint'),
                 ('run_uuid', 'double'),
                 ('sensor_type', 'string')]

# COMMAND ----------

# Setting an Index Column
df_raw =df_raw.withColumn("id", F.monotonically_increasing_id())

# Construct timestamp object - representing ms time
df_raw = df_raw.withColumn("ts", F.to_timestamp(df_raw["time"]))
df_raw = df_raw.withColumn("ts", F.col("ts").cast("double")*1000)
df_raw = df_raw.withColumn("ts", F.col("ts").cast("long"))
df_raw = df_raw.withColumn("date", F.to_date(F.col('time')).cast("string"))
df_raw = df_raw.drop('time') # Remove unnecessary time column - bases covered by ts and date. 

# Convert run_uuid to long dtype
df_raw = df_raw.withColumn("run_uuid", F.col("run_uuid").cast("long"))

display(df_raw)

# COMMAND ----------

# Enforece lower case text fields
df_raw = df_raw.withColumn('field', F.lower('field'))
df_raw = df_raw.withColumn('sensor_type', F.lower('sensor_type'))

# Remove white space
df_raw = df_raw.withColumn('field', F.trim(F.col('field')))
df_raw = df_raw.withColumn('sensor_type', F.trim(F.col('sensor_type')))

# COMMAND ----------

# Forward fill in missing value records by robot_id, field using last known value
# Define window of operation

def forwardFillImputer(df, cols=['value']):
    w1 = Window.partitionBy('run_uuid', 'field').orderBy('ts')
    df_new = df.select([ c for c in df.columns if c not in cols ] + [ F.last(c,True).over(w1).alias(c) for c in cols ])
    return df_new

df_raw_ff = forwardFillImputer(df_raw)
# Quick check
# display(df_raw_ff)
 

# COMMAND ----------

# Drop table IF Exists
# - Truncating might be faster than dropping / overwriting completely
spark.sql("DROP TABLE IF EXISTS " + table_name_result)

# Create data table if the destination table does not exist
spark.sql("CREATE TABLE IF NOT EXISTS " + table_name_result + " (" \
  "id LONG, " + \
  "run_uuid LONG," +\
  "ts LONG, " + \
  "date STRING, " + \
  "robot_id LONG, " + \
  "field STRING, " + \
  "sensor_type STRING, " + \
  "value DOUBLE)"
)

# Write latest clean data to table
df_raw_ff.write.mode('overwrite').format('delta').saveAsTable(table_name_result)
