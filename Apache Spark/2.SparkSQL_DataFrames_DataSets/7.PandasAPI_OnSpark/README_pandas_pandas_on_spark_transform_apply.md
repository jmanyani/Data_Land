### **Pandas on Spark - transform() and apply()**



This script uses PySpark with the Pandas API on Spark (pyspark.pandas) to perform DataFrame transformations similar to native pandas operations. 



**It includes:**



* Creating a Spark DataFrame using pyspark.pandas
* Using .transform() to modify values element-wise
* Using .apply() to categorize data with a custom function
* Creating a new column by combining existing ones vectorially





**Requirements**



* Python 3.x
* PySpark 3.2+
* PYARROW\_IGNORE\_TIMEZONE=1 environment variable is set to avoid timezone issues



**Key Concepts**



* pyspark.pandas brings the pandas experience to Spark
* .transform() applies a function element-wise to a Series
* .apply() can run custom logic on each element (slower, but flexible)
* Vectorized operations are preferred when possible for better performance
