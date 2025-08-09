from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

# Create SparkSession (set warehouse dir for Windows compatibility)
spark = SparkSession.builder \
    .appName("StructuredStreaming") \
    .config("spark.sql.warehouse.dir", "c://temp") \
    .getOrCreate()

# Read streaming text data from logs directory (adjust path as needed)
accessLines = spark.readStream.text("logs")

# Regex patterns to parse Apache logs (common log format)
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

# Extract fields from log lines using regexp_extract
logsDF = accessLines.select(
    regexp_extract('value', hostExp, 1).alias('host'),
    regexp_extract('value', timeExp, 1).alias('timestamp'),
    regexp_extract('value', generalExp, 1).alias('method'),
    regexp_extract('value', generalExp, 2).alias('endpoint'),
    regexp_extract('value', generalExp, 3).alias('protocol'),
    regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
    regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size')
)

# Group and count by HTTP status codes in streaming manner
statusCountsDF = logsDF.groupBy('status').count()

# Start streaming query: output to console, complete mode to show all counts
query = statusCountsDF.writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("status_counts") \
    .start()

query.awaitTermination()

spark.stop()
