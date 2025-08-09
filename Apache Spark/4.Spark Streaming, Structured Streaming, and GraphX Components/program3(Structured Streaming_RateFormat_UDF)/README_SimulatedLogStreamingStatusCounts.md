### 📄README: Simulated Log Streaming with PySpark and UDF

This script demonstrates how to simulate real-time log data in PySpark using a combination of the rate streaming source and a user-defined function (UDF) to pull random lines from a static Apache access log file. The parsed log data is then analyzed for HTTP status code distribution using Structured Streaming.



**What this script covers:**

* Creating a Spark session for Structured Streaming
* Using the rate source to simulate a stream of log events at a fixed rate
* Defining a custom UDF (get\_random\_log\_line) to return a random line from an existing log file
* Injecting simulated log lines into a streaming DataFrame
* Extracting key fields from Apache logs using regexp\_extract, including:



1. Host, Timestamp, Method, Endpoint, Protocol, Status Code, and Content Size
2. Aggregating log data by HTTP status code in real time
3. Displaying live results to the console



**Requirements:**

* Python 3.x
* PySpark
* A local file named access\_log.txt in the project directory with sample log entries



**Key Concepts:**

* rate source simulates streaming data by emitting rows at a fixed rate
* UDF (User Defined Function) allows dynamic injection of log data into a stream
* regexp\_extract is used to parse semi-structured log lines
* Structured Streaming provides a fault-tolerant, scalable streaming framework
* outputMode("complete") outputs full aggregation results at each trigger
