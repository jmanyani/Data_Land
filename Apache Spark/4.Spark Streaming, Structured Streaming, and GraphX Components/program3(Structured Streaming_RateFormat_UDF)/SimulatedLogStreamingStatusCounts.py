import random
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, regexp_extract
from pyspark.sql.types import StringType

LOG_PATH = "./access_log.txt"

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("LogStreamingSimulator") \
    .getOrCreate()

@udf(StringType())
def sample_log_line():
    """Returns a random complete line from the log file."""
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
                file.readline()  # Skip partial line
                line = file.readline().strip()
                if line:
                    return line
    except Exception as ex:
        print(f"Error reading log file: {ex}")
        return None

# Create a streaming DataFrame that emits rows at a fixed rate
rate_source = spark.readStream.format("rate").option("rowsPerSecond", 5).load()

# Add a random log line column using our UDF
streamed_logs = rate_source.withColumn("raw_log", sample_log_line())

# Regular expressions for parsing log fields
pattern_content_size = r'\s(\d+)$'
pattern_status = r'\s(\d{3})\s'
pattern_request = r'\"(\S+)\s(\S+)\s*(\S*)\"'
pattern_time = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
pattern_host = r'(^\S+\.[\S+\.]+\S+)\s'

# Extract structured columns from raw log lines
structured_logs = streamed_logs.select(
    regexp_extract('raw_log', pattern_host, 1).alias('client_host'),
    regexp_extract('raw_log', pattern_time, 1).alias('request_time'),
    regexp_extract('raw_log', pattern_request, 1).alias('http_method'),
    regexp_extract('raw_log', pattern_request, 2).alias('resource'),
    regexp_extract('raw_log', pattern_request, 3).alias('http_protocol'),
    regexp_extract('raw_log', pattern_status, 1).cast('integer').alias('http_status'),
    regexp_extract('raw_log', pattern_content_size, 1).cast('integer').alias('response_size')
)

# Aggregate counts by HTTP status code
status_aggregation = structured_logs.groupBy('http_status').count()

# Start streaming query to print results to console
query = status_aggregation.writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("status_counts") \
    .start()

query.awaitTermination()
spark.stop()
