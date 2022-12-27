# Databricks notebook source
# Imports
import pyspark.sql.functions as F
from pyspark.sql import Window
import sys
import math

# Source data from loacal data store
table_name_source = 'default.load_cell_additional_features'
table_name_source_runtime = 'default.load_cell_runtime_stats'
table_name_result = 'default.load_cell_total_velocity'

df = spark.sql("SELECT * FROM " + table_name_source)

robots = spark.sql("SELECT distinct robot_id  FROM " + table_name_source_runtime)
display(df) # Check output

# COMMAND ----------

filtered_columns = [col for col in df.columns if (( col.startswith("v") ) | ( col.startswith('run_uuid'))| ( col.startswith('ts')))]
df = df.select(*filtered_columns)

value_columns = [col for col in filtered_columns if col not in ['run_uuid','ts']]

df.show()

# COMMAND ----------

for robot in robots.select(F.collect_list('robot_id')).first()[0]: 
    vx = 'vx_' + str(robot)
    vy = 'vy_' + str(robot)
    vz = 'vz_' + str(robot)
    v_total = 'v' + str(robot) + '_total'
    # velocity
    df = df.withColumn(v_total,F.sqrt((F.col(vx)**2) + (F.col(vy)**2) +  (F.col(vz)**2)))
    
    df = df.drop(*(vx, vy, vz))
    
display(df)

# COMMAND ----------

# Drop table IF Exists
# - Truncating might be faster than dropping / overwriting completely
spark.sql("DROP TABLE IF EXISTS " + table_name_result)

# Write latest clean data to table
df.write.mode('overwrite').format('delta').saveAsTable(table_name_result)
