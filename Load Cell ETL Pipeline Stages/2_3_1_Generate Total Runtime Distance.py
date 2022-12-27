# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql import Window
import sys

# Source data from loacal data store
table_name_source = 'default.load_cell_pivot_clean'
table_name_source_runtime = 'default.load_cell_runtime_stats'
table_name_result = 'default.load_cell_total_distance'
df = spark.sql("SELECT * FROM " + table_name_source)
robots = spark.sql("SELECT distinct robot_id  FROM " + table_name_source_runtime)

display(df) # Check output

# COMMAND ----------

filtered_columns = [col for col in  df.columns if not col.startswith('f')]
df = df.select(filtered_columns)

# COMMAND ----------

for robot in robots.select(F.collect_list('robot_id')).first()[0]: 
    x = 'x_' + str(robot)
    y = 'y_' + str(robot)
    z = 'z_' + str(robot)
    total_dist = 'distance' + str(robot) + '_total'
    # distance
    df = df.withColumn(total_dist,F.sqrt(((F.col(x) - F.lag(x).over(Window.partitionBy('run_uuid').orderBy("ts")))**2) + ((F.col(y)- F.lag(y).over(Window.partitionBy('run_uuid').orderBy("ts")))**2) +  ((F.col(z) - F.lag(z).over(Window.partitionBy('run_uuid').orderBy("ts")))**2)))
    
    df = df.drop(*(x, y, z))
    
df = df.drop('ts')


# COMMAND ----------

value_columns = [col for col in  df.columns if col not in ['run_uuid', 'ts']]
df = df.groupBy('run_uuid').agg(*(F.sum(c).alias(c) for c in value_columns))

# COMMAND ----------

df = spark.createDataFrame(df.toPandas().melt(id_vars=['run_uuid'], value_vars=value_columns,
        var_name='robot_id', value_name='distance'))


# COMMAND ----------

df = df.withColumn('robot_id', F.regexp_extract("robot_id", "\d+", 0))

# COMMAND ----------

# Drop table IF Exists
# - Truncating might be faster than dropping / overwriting completely
spark.sql("DROP TABLE IF EXISTS " + table_name_result)

# Write latest clean data to table
df.write.mode('overwrite').format('delta').saveAsTable(table_name_result)
