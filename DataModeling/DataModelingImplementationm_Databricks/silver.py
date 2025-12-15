# Databricks notebook source
spark.sql("""
select *,
upper(customer_name) as customer_name_upper,
date(current_timestamp()) as processed_date

 from datamodeling.bronze.bronze_table

 """).createOrReplaceTempView("silver_source")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_source

# COMMAND ----------

if spark.catalog.tableExists("datamodeling.silver.silver_table"):
    pass
else:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS datamodeling.silver.silver_table
        AS
        SELECT * FROM silver_source
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC MERGE USING SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodeling.silver.silver_table
# MAGIC USING silver_source
# MAGIC ON datamodeling.silver.silver_table.Order_id= silver_source.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from datamodeling.silver.silver_table

# COMMAND ----------

