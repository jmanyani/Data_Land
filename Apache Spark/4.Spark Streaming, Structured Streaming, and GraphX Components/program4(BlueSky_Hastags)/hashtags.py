import random
import os
import re
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

DATA_FILE = "./bluesky.jsonl"

# Initialize Spark session
spark = SparkSession.builder.appName("RealtimeHashtagTrends").getOrCreate()

@udf(StringType())
def fetch_random_line():
    """Fetch a random JSON line from file without loading whole file in memory."""
    try:
        if not os.path.exists(DATA_FILE):
            return None
        size = os.path.getsize(DATA_FILE)
        if size == 0:
            return None
        with open(DATA_FILE, "r") as file:
            while True:
                pos = random.randint(0, size - 1)
                file.seek(pos)
                file.readline()  # skip partial line
                line = file.readline().strip()
                if line:
                    return line
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

# Custom user-defined table function to extract hashtags from JSON lines
class HashtagUDTF:
    def eval(self, json_str: str):
        try:
            obj = json.loads(json_str)
            text = obj.get("text", "")
            if text:
                tags = re.findall(r"#\w+", text)
                for tag in tags:
                    yield (tag.lower(),)
        except Exception:
            return

# Register UDTF with Spark
spark.udtf.register("extract_hashtags", HashtagUDTF)

# Simulate streaming by reading from rate source
rate_stream = spark.readStream.format("rate").option("rowsPerSecond", 50).load()

# Add a column with random JSON lines
posts_stream = rate_stream.withColumn("json_str", fetch_random_line())

# Create temp view for SQL querying
posts_stream.createOrReplaceTempView("posts_raw")

# Extract hashtags using lateral join with UDTF
hashtags_sql = """
    SELECT hashtag
    FROM posts_raw,
    LATERAL extract_hashtags(json_str)
"""

hashtags_df = spark.sql(hashtags_sql)
hashtags_df.createOrReplaceTempView("hashtags")

# Query top trending hashtags
top_tags_sql = """
    SELECT hashtag, COUNT(*) AS frequency
    FROM hashtags
    WHERE hashtag IS NOT NULL
    GROUP BY hashtag
    ORDER BY frequency DESC
    LIMIT 10
"""

top_tags_df = spark.sql(top_tags_sql)

# Start streaming query to console
query = (
    top_tags_df.writeStream
    .outputMode("complete")
    .format("console")
    .queryName("trending_hashtags")
    .start()
)

query.awaitTermination()
spark.stop()
