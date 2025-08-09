### 📄README: Sliding Window Endpoint Analysis with PySpark Structured Streaming

This script extends PySpark's Structured Streaming capabilities to perform time-windowed analysis of web server logs. It continuously monitors a log directory, extracts structured fields from raw log lines, and maintains a live count of requests per endpoint over sliding time windows.



**What this script covers:**

* Initializing a SparkSession for real-time stream processing
* Reading live data from the logs directory using readStream
* Extracting key fields from unstructured Apache log lines, including:



1. Host, Timestamp, HTTP Method, Endpoint, Protocol, Status Code, Content Size
2. Appending a processing timestamp (eventTime) to each row
3. Performing sliding window aggregations:
4. 30-second window duration
5. 10-second sliding interval
6. Sorting and displaying the most requested endpoints in real time



**Requirements:**

* Python 3.x
* PySpark
* A local logs directory with continuously added log entries (can be simulated with tools like nc or file watchers)



**Key Concepts:**

* Sliding Windows allow analysis over overlapping time intervals using window()
* current\_timestamp() adds event-time metadata to each log line
* Structured Streaming enables scalable, continuous data processing
* outputMode("complete") is used to output full results each time the window slides
