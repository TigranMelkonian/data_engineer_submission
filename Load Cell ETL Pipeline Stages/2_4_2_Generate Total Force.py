# Databricks notebook source
# Imports
import pyspark.sql.functions as F
from pyspark.sql import Window
import sys
import math

# Source data from loacal data store
table_name_source = 'default.load_cell_pivot_clean'
table_name_source_runtime = 'default.load_cell_runtime_stats'
table_name_result = 'default.load_cell_total_force'

df = spark.sql("SELECT * FROM " + table_name_source)

robots = spark.sql("SELECT distinct robot_id  FROM " + table_name_source_runtime)
display(df) # Check output

# COMMAND ----------

filtered_columns = [col for col in df.columns if (( col.startswith("f") ) | ( col.startswith('run_uuid'))| ( col.startswith('ts')))]
df = df.select(*filtered_columns)

value_columns = [col for col in filtered_columns if col not in ['run_uuid','ts']]

df.show()

# COMMAND ----------

for robot in robots.select(F.collect_list('robot_id')).first()[0]: 
    fx = 'fx_' + str(robot)
    fy = 'fy_' + str(robot)
    fz = 'fz_' + str(robot)
    f_total = 'f' + str(robot) + '_total'
    # velocity
    df = df.withColumn(f_total,F.sqrt((F.col(fx)**2) + (F.col(fy)**2) +  (F.col(fz)**2)))
    
    df = df.drop(*(fx, fy, fz))
    
display(df)

# COMMAND ----------

# Drop table IF Exists
# - Truncating might be faster than dropping / overwriting completely
spark.sql("DROP TABLE IF EXISTS " + table_name_result)

# Write latest clean data to table
df.write.mode('overwrite').format('delta').saveAsTable(table_name_result)
