# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql import Window
import sys
# Source data from loacal data store
table_name_source = 'default.load_cell_clean'
table_name_result = 'default.load_cell_pivot_clean'
df = spark.sql("SELECT ts, cast(run_uuid as long), robot_id, field, value FROM " + table_name_source)
display(df) # Check output

# COMMAND ----------

df = df.withColumn('tmp', F.concat(F.col('field'),F.lit('_'), F.col('robot_id')))
df_pivot = df.groupBy("run_uuid", "ts").pivot("tmp").agg(F.first("value")).sort(['run_uuid','ts'], ascending=True)
display(df_pivot)

# COMMAND ----------

value_columns = (df_pivot.columns)
value_columns = [col for col in value_columns if col not in ['run_uuid','ts']]

def forwardFillImputer(df, cols=value_columns):
    w1 = Window.partitionBy('run_uuid').orderBy('ts')
    df_new = df.select([ c for c in df.columns if c not in cols ] + [ F.last(c,True).over(w1).alias(c) for c in cols ])
    return df_new

def backwardFillImputer(df, cols=value_columns):
    w2 = Window.partitionBy('run_uuid').orderBy('ts').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    df_new = df.select([ c for c in df.columns if c not in cols ] + [ F.first(c,True).over(w2).alias(c) for c in cols ])
    return df_new

df_pivot_ff = forwardFillImputer(df_pivot)
df_pivot_bf = backwardFillImputer(df_pivot)

# COMMAND ----------

df_pivot_filled_union = df_pivot_ff.union(df_pivot_bf)
df_pivot_clean = df_pivot_filled_union.groupby('run_uuid', 'ts').agg(*(F.avg(c).alias(c) for c in value_columns))
df_pivot_clean = df_pivot_clean.distinct()

# COMMAND ----------

# Drop table IF Exists
# - Truncating might be faster than dropping / overwriting completely
spark.sql("DROP TABLE IF EXISTS " + table_name_result)

# Write latest clean data to table
df_pivot_clean.write.mode('overwrite').format('delta').saveAsTable(table_name_result)
