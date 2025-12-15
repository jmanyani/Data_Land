# Databricks notebook source
# MAGIC %md
# MAGIC ###**SCD TYPE 1 **

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Create the table
# MAGIC CREATE TABLE datamodeling.default.scdtyp1_source
# MAGIC (
# MAGIC     prod_id INT,
# MAGIC     prod_name STRING,
# MAGIC     prod_cat STRING,
# MAGIC     processDate DATE
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Insert data into the table
# MAGIC INSERT INTO datamodeling.default.scdtyp1_source
# MAGIC VALUES
# MAGIC (1, 'prod1', 'cat1', CURRENT_DATE()),
# MAGIC (2, 'prod2', 'cat2', CURRENT_DATE()),
# MAGIC (3, 'prod3', 'cat3', CURRENT_DATE());

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamodeling.default.scdtyp1_source

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE datamodeling.gold.scdtyp1_table
# MAGIC (
# MAGIC     prod_id INT,
# MAGIC     prod_name STRING,
# MAGIC     prod_cat STRING,
# MAGIC     processDate DATE
# MAGIC );

# COMMAND ----------

spark.sql("select * from datamodeling.default.scdtyp1_source").createOrReplaceTempView("src")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodeling.gold.scdtyp1_table AS trg
# MAGIC USING datamodeling.default.scdtyp1_source AS src
# MAGIC ON trg.prod_id = src.prod_id
# MAGIC WHEN MATCHED AND src.processDate >= trg.processDate THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT  *

# COMMAND ----------

# MAGIC %sql
# MAGIC select* from  datamodeling.gold.scdtyp1_table
# MAGIC
# MAGIC

# COMMAND ----------

