from pyspark.sql import SparkSession
import os
import pyspark.pandas as ps  # pandas API on Spark

# Suppress timezone warnings
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

# Initialize SparkSession with required configs
spark_session = SparkSession.builder \
    .appName("PandasAPIonSparkTransformApply") \
    .config("spark.sql.ansi.enabled", "false") \
    .config("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
    .getOrCreate()

# Create a pandas-on-Spark DataFrame
ps_df = ps.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45],
    "salary": [50000, 60000, 75000, 80000, 120000]
})

print("Original DataFrame:")
print(ps_df)

# 1. Add 10 years to age using transform (element-wise)
ps_df["age_in_10_years"] = ps_df["age"].transform(lambda x: x + 10)

# 2. Categorize salary with apply function
def salary_category(salary):
    if salary < 60000:
        return "Low"
    elif salary < 100000:
        return "Medium"
    else:
        return "High"

ps_df["salary_category"] = ps_df["salary"].apply(salary_category)

# 3. Create a combined column with name and age as string
ps_df["name_with_age"] = ps_df["name"] + " (" + ps_df["age"].astype(str) + " years old)"

print("\nDataFrame after transformations:")
print(ps_df)

# Stop Spark session
spark_session.stop()
