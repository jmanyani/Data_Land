import random
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType

# Path to log file — configurable via env var or default
LOG_PATH = os.getenv("LOG_FILE_PATH", "./access_log.txt")

# Initialize Spark session
spark = SparkSession.builder.appName("RealTimeLogAnalysis").getOrCreate()

# UDF to pick a random log entry from the file
@udf(StringType())
def fetch_random_log_entry():
    try:
        if not os.path.isfile(LOG_PATH):
            return None
        
        size = os.path.getsize(LOG_PATH)
        if size == 0:
            return None
        
        with open(LOG_PATH, "r") as file:
            while True:
                pos = random.randint(0, size - 1)
                file.seek(pos)
                file.readline()  # skip partial line
                line = file.readline().strip()
                if line:
                    return line
    except Exception:
        return None

# Regular expressions for log parsing
regex_host = r'(^\S+\.[\S+\.]+\S+)\s'
regex_time = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
regex_request = r'\"(\S+)\s(\S+)\s*(\S*)\"'
regex_status = r'\s(\d{3})\s'
regex_size = r'\s(\d+)$'
regex_agent = r'\"[^\"]*\" \"([^\"]+)\"'

# General extraction helper
def extract_field(log_line, pattern, group_num=1, to_int=False):
    match = re.search(pattern, log_line) if log_line else None
    if match:
        val = match.group(group_num)
        if to_int:
            try:
                return int(val)
            except:
                return None
        return val
    return None

# Define UDFs for each field extraction
extract_host_udf = udf(lambda x: extract_field(x, regex_host), StringType())
extract_time_udf = udf(lambda x: extract_field(x, regex_time), StringType())
extract_method_udf = udf(lambda x: extract_field(x, regex_request, 1), StringType())
extract_endpoint_udf = udf(lambda x: extract_field(x, regex_request, 2), StringType())
extract_protocol_udf = udf(lambda x: extract_field(x, regex_request, 3), StringType())
extract_status_udf = udf(lambda x: extract_field(x, regex_status, 1, True), IntegerType())
extract_size_udf = udf(lambda x: extract_field(x, regex_size, 1, True), IntegerType())
extract_agent_udf = udf(lambda x: extract_field(x, regex_agent), StringType())

# Read streaming source that triggers at 5 rows/sec
rate_stream = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# Add log line column from random log entries
logs_stream = rate_stream.withColumn("raw_log", fetch_random_log_entry())

# Parse log lines using UDFs
structured_logs = logs_stream.select(
    extract_host_udf(col("raw_log")).alias("host"),
    extract_time_udf(col("raw_log")).alias("timestamp"),
    extract_method_udf(col("raw_log")).alias("method"),
    extract_endpoint_udf(col("raw_log")).alias("endpoint"),
    extract_protocol_udf(col("raw_log")).alias("protocol"),
    extract_status_udf(col("raw_log")).alias("status"),
    extract_size_udf(col("raw_log")).alias("content_size"),
    extract_agent_udf(col("raw_log")).alias("user_agent")
)

# Register temporary view for SQL querying
structured_logs.createOrReplaceTempView("logs_view")

# SQL to aggregate top user agents seen so far
top_agents_query = """
SELECT user_agent, COUNT(*) as occurrences
FROM logs_view
WHERE user_agent IS NOT NULL
GROUP BY user_agent
ORDER BY occurrences DESC
LIMIT 10
"""

top_agents = spark.sql(top_agents_query)

# Output the results to console in complete mode
query = top_agents.writeStream.outputMode("complete").format("console").queryName("top_agents_stream").start()

query.awaitTermination()

spark.stop()
