# Databricks notebook source
if spark.catalog.tableExists("datamodeling.bronze.bronze_table"):
  pass
  last_load_date=spark.sql("select max(order_date) from datamodeling.bronze.bronze_table").collect()[0][0]
else:
    last_load_date='1000-01-01'

# COMMAND ----------

print(last_load_date)

# COMMAND ----------

spark.sql(f"""
SELECT *
FROM datamodeling.default.source_data
WHERE order_date > '{last_load_date}'
""").createOrReplaceTempView("bronze_source")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_source

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table datamodeling.bronze.bronze_table
# MAGIC as
# MAGIC select * from bronze_source

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamodeling.bronze.bronze_table

# COMMAND ----------

