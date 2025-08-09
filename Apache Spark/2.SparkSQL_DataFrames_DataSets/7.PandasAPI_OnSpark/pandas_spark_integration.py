from pyspark.sql import SparkSession
import os
import pandas as pd
import pyspark.pandas as ps  # pandas API on Spark

# Suppress timezone warnings
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

# Initialize Spark session with configs
spark_session = SparkSession.builder \
    .appName("PandasSparkIntegration") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
    .getOrCreate()

# 1. Create a Pandas DataFrame
pandas_df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45]
})

print("Pandas DataFrame:")
print(pandas_df)

# 2. Convert Pandas DataFrame to Spark DataFrame
spark_df = spark_session.createDataFrame(pandas_df)

print("\nSchema of Spark DataFrame:")
spark_df.printSchema()

print("\nSpark DataFrame:")
spark_df.show()

# 3. Filter Spark DataFrame for age > 30
filtered_spark_df = spark_df.filter(spark_df.age > 30)
print("\nFiltered Spark DataFrame (age > 30):")
filtered_spark_df.show()

# 4. Convert filtered Spark DataFrame back to Pandas DataFrame
converted_pandas_df = filtered_spark_df.toPandas()
print("\nConverted Pandas DataFrame:")
print(converted_pandas_df)

# 5. Create pandas-on-Spark DataFrame from original Pandas DataFrame
ps_df = ps.DataFrame(pandas_df)

# Perform pandas-like operation on pandas-on-Spark DataFrame (increment age by 1)
print("\nUsing pandas-on-Spark (incrementing age by 1):")
ps_df["age"] = ps_df["age"] + 1
print(ps_df)

# 6. Convert pandas-on-Spark DataFrame to Spark DataFrame
converted_spark_df = ps_df.to_spark()
print("\nConverted Spark DataFrame from pandas-on-Spark:")
converted_spark_df.show()

# Stop Spark session
spark_session.stop()
