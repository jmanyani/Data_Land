### **📄README: Real-Time Log Analysis with PySpark Structured Streaming**

This script demonstrates how to build a real-time streaming pipeline using PySpark’s Structured Streaming API. It continuously monitors a directory for incoming web server log files, parses the logs using regular expressions, and aggregates the data by HTTP status codes.



**What this script covers:**

* Creating a SparkSession for structured streaming
* Reading new log lines in real time from the logs directory
* Parsing common Apache log fields using regexp\_extract, including:



1. Hostname
2. Timestamp
3. HTTP Method, Endpoint, and Protocol
4. Status code
5. Content size



* Aggregating the logs to count the number of requests per HTTP status code
* Writing the aggregated results to the console in real-time



**Requirements:**

* Python 3.x
* PySpark
* A local directory named logs where new log files will appear in real time (e.g., via nc or another log generator)



**Key Concepts:**

* Structured Streaming in PySpark enables scalable and fault-tolerant stream processing
* regexp\_extract is used to extract structured fields from unstructured log text
* Streaming DataFrames allow operations like groupBy and writeStream in real-time
* outputMode("complete") is used to continuously output the full aggregation result
