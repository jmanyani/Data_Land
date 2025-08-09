from pyspark.sql import SparkSession
import pyspark.sql.functions as func

spark = SparkSession.builder.appName("EndpointFrequencyStreaming").getOrCreate()

# Stream text data from logs directory (source path abstracted)
accessLines = spark.readStream.text("logs")

# Extract relevant fields from Apache-style logs using regex
logsDF = accessLines.select(
    func.regexp_extract('value', r'(^\S+\.[\S+\.]+\S+)\s', 1).alias('host'),
    func.regexp_extract('value', r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})\]', 1).alias('timestamp'),
    func.regexp_extract('value', r'\"(\S+)\s(\S+)\s*(\S*)\"', 1).alias('method'),
    func.regexp_extract('value', r'\"(\S+)\s(\S+)\s*(\S*)\"', 2).alias('endpoint'),
    func.regexp_extract('value', r'\"(\S+)\s(\S+)\s*(\S*)\"', 3).alias('protocol'),
    func.regexp_extract('value', r'\s(\d{3})\s', 1).cast('integer').alias('status'),
    func.regexp_extract('value', r'\s(\d+)$', 1).cast('integer').alias('content_size')
)

# Add event timestamp for sliding window aggregation
logsDF2 = logsDF.withColumn("eventTime", func.current_timestamp())

# Sliding window count of endpoints over 30s windows sliding every 10s
endpointCounts = logsDF2.groupBy(
    func.window(func.col("eventTime"), "30 seconds", "10 seconds"),
    func.col("endpoint")
).count()

# Order descending by count
sortedEndpointCounts = endpointCounts.orderBy(func.col("count").desc())

# Output to console in complete mode
query = sortedEndpointCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("endpoint_counts") \
    .start()

query.awaitTermination()

spark.stop()
