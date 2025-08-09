### 📄README: Real-Time Hashtag Analysis from JSONL Streams with PySpark

This PySpark script processes simulated social media posts stored in newline-delimited JSON (.jsonl) format. It extracts hashtags from the text field using a user-defined table function (UDTF) and tracks the top trending hashtags in real time using Spark Structured Streaming.



**What this script covers:**

* Initializing a Spark session
* Using PySpark’s rate source to simulate streaming input
* Reading random JSON lines from a local .jsonl file (bluesky.jsonl) using a custom UDF
* Defining and registering a UDTF (HashtagExtractor) to extract multiple hashtags from a post
* Running a LATERAL SQL query to flatten and collect hashtags from incoming JSON posts
* Aggregating and displaying the top 10 hashtags as they emerge in the stream



**Requirements:**

* Python 3.x
* PySpark (version 3.4 or later for UDTF support)
* A local file bluesky.jsonl with JSON-formatted social media posts (must contain a "text" field)



**Key Concepts:**

* User-Defined Table Functions (UDTFs) allow outputting multiple rows per input, ideal for extracting multiple hashtags
* rate source simulates a real-time stream for testing purposes
* Structured Streaming makes it possible to process and analyze data as it arrives
* LATERAL joins enable applying the UDTF to each incoming row
* outputMode("complete") provides full ranked output on each trigger
