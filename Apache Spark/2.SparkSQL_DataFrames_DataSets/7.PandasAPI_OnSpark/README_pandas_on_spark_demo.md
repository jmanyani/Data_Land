### **README : Using Pandas API on Spark**



**Description:**



This script explores the Pandas API on Spark (also called pandas-on-Spark), which allows using Pandas-like syntax for large-scale data processing on Spark.



**What it does:**



* Creates a pandas-on-Spark DataFrame with sample employee data.
* Computes average age using Pandas-style syntax.
* Prints summary statistics using .describe().
* Adds a new column salary\_after\_increment by applying an operation.
* Filters rows with age > 30
* Converts the DataFrame to a Spark DataFrame.
* Converts it back to a pandas-on-Spark DataFrame.



**Key Concepts Used:**



* pyspark.pandas.DataFrame
* .mean() and .describe()
* Column operations and filtering
* .to\_spark() and ps.DataFrame(spark\_df)



**Requirements:**

* Spark 3.2+
* Python 3.8+
* pyspark, pyarrow, and pandas
* Set environment variable PYARROW\_IGNORE\_TIMEZONE=1 to suppress timezone warnings
