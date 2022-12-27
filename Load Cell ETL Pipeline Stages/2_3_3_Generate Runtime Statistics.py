# Databricks notebook source
# Imports
import pyspark.sql.functions as F
from pyspark.sql import Window
import sys

# Source data from loacal data store
table_name_source = 'default.load_cell_clean'
table_name_result = 'default.load_cell_runtime_stats'

df = spark.sql("SELECT DISTINCT run_uuid, robot_id, min(ts) as run_starttime, max(ts) as run_endtime, (max(ts) - min(ts))/1000 as total_runtime_s FROM " + table_name_source + " GROUP BY run_uuid, robot_id")
# display(df) # Check output

# COMMAND ----------

# Drop table IF Exists
# - Truncating might be faster than dropping / overwriting completely
spark.sql("DROP TABLE IF EXISTS " + table_name_result)

# Write latest clean data to table
df.write.mode('overwrite').format('delta').saveAsTable(table_name_result)
