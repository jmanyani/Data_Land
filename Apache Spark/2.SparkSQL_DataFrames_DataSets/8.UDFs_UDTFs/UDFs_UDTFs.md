### **README : Python UDTF and UDF for Hashtag Extraction**



This script shows how to create and register Python UDTFs and UDFs in PySpark for text processing tasks:



* Defines a UDTF HashtagExtractor to extract all hashtags from a text string, returning multiple rows per input.
* Defines a UDF count\_hashtags to count the number of hashtags in a text string.
* Demonstrates running both UDTF and UDF via Spark SQL queries.
* Shows how to apply these functions on Spark DataFrames, including using LATERAL JOIN with the UDTF.



**Requirements**



* Python 3.x
* PySpark 3.4+ (UDTF support requires newer versions)
* Enable Python UDTF via Spark config spark.sql.execution.pythonUDTF.enabled=true



**Key Concepts**



* UDFs (User-Defined Functions) return single scalar values per input row
* UDTFs (User-Defined Table Functions) return zero or more rows per input row
* Use UDTFs with LATERAL JOIN in Spark SQL to explode multiple outputs
* Register UDFs and UDTFs to use them in SQL or DataFrame APIs
