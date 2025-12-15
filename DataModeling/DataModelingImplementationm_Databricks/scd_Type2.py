# Databricks notebook source
# MAGIC %md
# MAGIC ###SCD TYPE 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source Table Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE datamodeling.default.scdtype2_source(
# MAGIC
# MAGIC prod_id INT,
# MAGIC prod_name STRING,
# MAGIC prod_cat STRING,
# MAGIC processDate DATE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO datamodeling.default.scdtype2_source
# MAGIC VALUES
# MAGIC (1,'prod1','cat1',CURRENT_DATE()),
# MAGIC (2,'prod2','cat2',CURRENT_DATE()),
# MAGIC (3,'prod3','cat3',CURRENT_DATE())

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamodeling.default.scdtyp1_source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Target Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE datamodeling.gold.scdtype2_table(
# MAGIC prod_id INT,
# MAGIC   prod_name STRING,
# MAGIC   prod_cat STRING,
# MAGIC   processDate DATE,
# MAGIC   start_date DATE,
# MAGIC   end_date DATE,
# MAGIC   is_current STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding additional 3 columns for SCD Type - 2 (Temp Table src)

# COMMAND ----------

spark.sql(""" 
SELECT * ,
current_timestamp() as start_date,
CAST('3000-01-01' AS TIMESTAMP) as end_date,
'Y' as is_current
FROM datamodeling.default.scdtype2_source        
          """).createOrReplaceTempView("src")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from src

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD TYPE 2 MERGE
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC STEP 1 : UPDATING THE EXISTING ROW 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodeling.gold.scdtype2_table AS trg
# MAGIC USING src
# MAGIC ON src.prod_id = trg.prod_id
# MAGIC AND trg.is_current='Y'
# MAGIC
# MAGIC --When we have new data with updates
# MAGIC WHEN MATCHED AND (
# MAGIC src.prod_cat<>trg.prod_cat OR
# MAGIC src.prod_name<>trg.prod_name OR 
# MAGIC src.processDate<>trg.processDate
# MAGIC )THEN
# MAGIC UPDATE SET
# MAGIC trg.is_current='N',
# MAGIC trg.end_date=current_timestamp()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 : ADDING THE NEW ROW

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodeling.gold.scdtype2_table AS trg
# MAGIC USING src
# MAGIC ON src.prod_id = trg.prod_id
# MAGIC AND trg.is_current='Y'
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC prod_id,
# MAGIC prod_name,
# MAGIC prod_cat,
# MAGIC processDate,
# MAGIC start_date,
# MAGIC end_date,
# MAGIC is_current
# MAGIC )VALUES (
# MAGIC    src.prod_id,
# MAGIC   src.prod_name,
# MAGIC   src.prod_cat,
# MAGIC   src.processDate,
# MAGIC   src.start_date,
# MAGIC   src.end_date,
# MAGIC   src.is_current
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from datamodeling.gold.scdtype2_table

# COMMAND ----------

# MAGIC %md
# MAGIC ###Testing By updating the prod_name of prod3

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE datamodeling.default.scdtype2_source
# MAGIC SET prod_name="Product3"
# MAGIC WHERE prod_id=3

# COMMAND ----------

