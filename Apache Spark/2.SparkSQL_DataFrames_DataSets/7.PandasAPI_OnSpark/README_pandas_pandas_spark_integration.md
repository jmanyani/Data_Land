**README : Pandas \& Spark Integration (pandas-on-Spark)**



**Description**:

This PySpark script explores interoperability between standard Pandas, PySpark DataFrames, and pandas-on-Spark (pyspark.pandas). It showcases converting between formats, performing transformations, and using scalable Pandas-like syntax for Spark-backed operations.



**Steps Performed:**



1. Create a sample Pandas DataFrame.
   
2. Convert it to a Spark DataFrame and print schema + contents.
   
3. Apply a simple filter on the Spark DataFrame (age > 30).
   
4. Convert the filtered Spark DataFrame back to Pandas.
   
5. Use pandas-on-Spark to increment a column's values.
   
6. Convert the pandas-on-Spark DataFrame back to a Spark DataFrame.
   
7. Print results at each stage for clarity.



**Key Features Used:**



* spark.createDataFrame()
* toPandas()
* pyspark.pandas.DataFrame()
* to\_spark()



**Requirements**:



* Apache Spark (3.2+ recommended)
* Python 3
* PyArrow installed
* pyspark.pandas (comes with Spark 3.2+, or install separately)
* Dataset is created programmatically (no external files needed)
