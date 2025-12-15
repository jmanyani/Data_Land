# Databricks notebook source
# MAGIC %md
# MAGIC **Dim Customers**

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table datamodeling.gold.DimCustomers
# MAGIC as
# MAGIC WITH rem_duplicates AS
# MAGIC (select distinct(customer_id),
# MAGIC customer_name,
# MAGIC customer_email,
# MAGIC customer_name_upper
# MAGIC from datamodeling.silver.silver_table)
# MAGIC
# MAGIC select *,row_number()over(order by customer_id) as DimCustomerKey
# MAGIC from rem_duplicates
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Dim Products**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.DimProducts AS
# MAGIC WITH rem_duplicates AS (
# MAGIC     SELECT DISTINCT
# MAGIC         product_id,
# MAGIC         product_name,
# MAGIC         product_category
# MAGIC     FROM datamodeling.silver.silver_table
# MAGIC )
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     row_number() OVER (ORDER BY product_id) AS DimProductKey
# MAGIC FROM rem_duplicates;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ****Dim Payments**
# MAGIC **

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.DimPaymentkey AS
# MAGIC WITH rem_duplicates AS (
# MAGIC     SELECT DISTINCT
# MAGIC         payment_type
# MAGIC     FROM datamodeling.silver.silver_table
# MAGIC )
# MAGIC SELECT
# MAGIC     payment_type,
# MAGIC     row_number() OVER (ORDER BY payment_type) AS DimPaymentKey
# MAGIC FROM rem_duplicates;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC DIM REGION

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.DimRegion AS
# MAGIC WITH rem_duplicates AS (
# MAGIC     SELECT DISTINCT
# MAGIC         country
# MAGIC     FROM datamodeling.silver.silver_table
# MAGIC )
# MAGIC SELECT
# MAGIC     country,
# MAGIC     row_number() OVER (ORDER BY country) AS DimRegionKey
# MAGIC FROM rem_duplicates;
# MAGIC
# MAGIC DimCustomerKey
# MAGIC DimProductKey
# MAGIC PaymentKey
# MAGIC RegionKey
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamodeling.gold.DimRegion

# COMMAND ----------

# MAGIC %md
# MAGIC DIM Sales

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.DimSales AS
# MAGIC SELECT
# MAGIC     row_number() over (order by order_id) as DimSaleKey,
# MAGIC     order_id,
# MAGIC     order_date,
# MAGIC     customer_id,
# MAGIC     customer_name,
# MAGIC     customer_email,
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_category,
# MAGIC     payment_type,
# MAGIC     country,
# MAGIC     last_updated,
# MAGIC     customer_name_upper,
# MAGIC     processed_date
# MAGIC FROM datamodeling.silver.silver_table;
# MAGIC
# MAGIC DimCustomerKey
# MAGIC DimProductKey
# MAGIC PaymentKey
# MAGIC RegionKey
# MAGIC DimSaleKey
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from datamodeling.gold.dimsales

# COMMAND ----------

# MAGIC %md
# MAGIC **FACT TABLE**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamodeling.silver.silver_table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.factSales AS
# MAGIC SELECT
# MAGIC     c.DimCustomerKey,
# MAGIC     p.DimProductKey,
# MAGIC     pmt.DimPaymentKey,
# MAGIC     r.DimRegionKey,
# MAGIC     ds.DimSaleKey,
# MAGIC     s.quantity,
# MAGIC     s.unit_price
# MAGIC FROM datamodeling.silver.silver_table s
# MAGIC LEFT JOIN datamodeling.gold.dimcustomers c
# MAGIC     ON s.customer_id = c.customer_id
# MAGIC LEFT JOIN datamodeling.gold.dimproducts p
# MAGIC     ON s.product_id = p.product_id
# MAGIC LEFT JOIN datamodeling.gold.dimpaymentkey pmt
# MAGIC     ON s.payment_type = pmt.payment_type
# MAGIC LEFT JOIN datamodeling.gold.dimregion r
# MAGIC     ON s.country = r.country
# MAGIC LEFT JOIN datamodeling.gold.dimsales ds
# MAGIC     ON s.order_id = ds.order_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamodeling.gold.factSales

# COMMAND ----------

