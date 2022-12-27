# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql import Window
import sys

# Source data from loacal data store
table_name_source = 'default.load_cell_pivot_clean'
table_name_source_runtime = 'default.load_cell_runtime_stats'
table_name_result = 'default.load_cell_additional_features'
robots = spark.sql("SELECT distinct robot_id  FROM " + table_name_source_runtime)
df = spark.sql("SELECT * FROM " + table_name_source)
display(df) # Check output

# COMMAND ----------

value_columns = [col for col in df.columns if (col not in ['run_uuid','ts']) and  (not col.startswith("f"))]
df = df.withColumn("ts_delta", (F.col('ts') - F.lag("ts").over(Window.partitionBy('run_uuid').orderBy("ts")))/1000)


# COMMAND ----------

for col in value_columns:
    df = df.withColumn("v" + col, (F.col(col) - F.lag(col).over(Window.partitionBy('run_uuid').orderBy("ts")))/F.col('ts_delta'))
    df = df.withColumn("a" + col, (F.col("v" + col) - F.lag("v" + col).over(Window.partitionBy('run_uuid').orderBy("ts")))/F.col('ts_delta'))
df = df.drop('ts_delta')

# COMMAND ----------

# Drop table IF Exists
# - Truncating might be faster than dropping / overwriting completely
spark.sql("DROP TABLE IF EXISTS " + table_name_result)

# Write latest clean data to table
df.write.mode('overwrite').format('delta').saveAsTable(table_name_result)
