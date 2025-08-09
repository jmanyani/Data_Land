### 📄README: Simulated Log Stream with UDF-Based Parsing and User Agent Analysis

This advanced PySpark script demonstrates how to simulate a live log stream and extract rich structured data from raw Apache log entries using custom UDFs. The script continuously monitors and ranks the top user agents encountered in the simulated traffic.



**What this script covers:**

* Initializing a SparkSession for structured streaming
* Using PySpark’s rate source to simulate streaming input
* Creating a custom UDF get\_random\_log\_line() to retrieve a random line from a local static access log file
* Defining UDFs for field-level parsing using regular expressions:



1. Host, Timestamp, Method, Endpoint, Protocol, Status Code, Content Size, User Agent
2. Registering UDFs and using them within Spark SQL
3. Creating a SQL view and querying structured fields
4. Aggregating and displaying the top 10 user agents in real time



**Requirements:**

* Python 3.x
* PySpark
* A local file named access\_log.txt in the same directory, containing Apache access logs



**Key Concepts:**

* UDFs (User Defined Functions) allow for detailed parsing of semi-structured log data using Python and regex
* Streaming from rate source simulates event generation without relying on real-time ingestion
* Spark SQL Integration enables flexible querying over structured streaming data
* outputMode("complete") outputs full rankings at every trigger
* Top-k Aggregation using GROUP BY, ORDER BY, and LIMIT tracks user agent frequency over time





