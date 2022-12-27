# Databricks notebook source
# Source data from loacal data store
load_cell_clean = 'default.load_cell_clean'
load_cell_pivot_clean = 'default.load_cell_pivot_clean'
load_cell_additional_features = 'default.load_cell_additional_features'
load_cell_runtime_stats = 'default.load_cell_runtime_stats'
load_cell_total_acceleration = 'default.load_cell_total_acceleration'
load_cell_total_distance = 'default.load_cell_total_distance'
load_cell_total_force = 'default.load_cell_total_force'
load_cell_total_velocity = 'default.load_cell_total_velocity'

processed_table_list = [load_cell_clean
             ,load_cell_pivot_clean
             ,load_cell_additional_features
             ,load_cell_runtime_stats
             ,load_cell_total_acceleration
             ,load_cell_total_distance
             ,load_cell_total_force
             ,load_cell_total_velocity]

# COMMAND ----------

table.split('.')[1]

# COMMAND ----------

for table in processed_table_list:
    df = spark.sql("SELECT * FROM " + table)
    file_name = table.split('.')[1]
    # Save table to processed s3 directory in s3 -  Will implement partitioning in the future
    df.write.mode("overwrite").parquet(f's3://machina-integ/load-cell/processed/{file_name}')
