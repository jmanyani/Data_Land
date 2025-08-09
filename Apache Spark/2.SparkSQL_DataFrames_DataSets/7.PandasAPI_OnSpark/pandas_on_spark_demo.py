from pyspark.sql import SparkSession
import os
import pyspark.pandas as ps  # pandas-on-Spark API

# Set environment variable to suppress timezone warnings
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

# Initialize Spark session with required configs
spark_session = SparkSession.builder \
    .appName("PandasAPIonSparkDemo") \
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

print("Pandas-on-Spark DataFrame:")
print(ps_df)

# Calculate average age
print("\nAverage Age:", ps_df["age"].mean())

# Show summary statistics
print("\nSummary Statistics:")
print(ps_df.describe())

# Add a new column for salary after 10% increment
ps_df["salary_after_increment"] = ps_df["salary"] * 1.1
print("\nDataFrame after Salary Increment:")
print(ps_df)

# Filter rows where age is greater than 30
filtered_df = ps_df[ps_df["age"] > 30]
print("\nFiltered DataFrame (age > 30):")
print(filtered_df)

# Convert pandas-on-Spark DataFrame to Spark DataFrame
spark_df = ps_df.to_spark()
print("\nConverted Spark DataFrame:")
spark_df.show()

# Convert Spark DataFrame back to pandas-on-Spark DataFrame
ps_df_reconverted = ps.DataFrame(spark_df)
print("\nReconverted Pandas-on-Spark DataFrame:")
print(ps_df_reconverted)

# Stop Spark session
spark_session.stop()
