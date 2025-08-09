from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf, udf
from pyspark.sql.types import IntegerType
import re

# ----------------------------
# User-Defined Table Function (UDTF) to extract hashtags from text
# ----------------------------
@udtf(returnType="hashtag: string")
class HashtagExtractorUDTF:
    def eval(self, text: str):
        """Yield all hashtags found in the input text."""
        if text:
            hashtags = re.findall(r"#\w+", text)
            for tag in hashtags:
                yield (tag,)

# ----------------------------
# User-Defined Function (UDF) to count hashtags in text
# ----------------------------
@udf(returnType=IntegerType())
def hashtag_count_udf(text: str):
    """Return number of hashtags in the input text."""
    if text:
        return len(re.findall(r"#\w+", text))
    return 0

# Initialize Spark session with UDTF enabled
spark_session = SparkSession.builder \
    .appName("UDTF_UDF_Hashtag_Example") \
    .config("spark.sql.execution.pythonUDTF.enabled", "true") \
    .getOrCreate()

# Register UDTF and UDF for Spark SQL
spark_session.udtf.register("extract_hashtags", HashtagExtractorUDTF)
spark_session.udf.register("count_hashtags", hashtag_count_udf)

# ----------------------------
# Example usage of UDTF in Spark SQL
# ----------------------------
print("\nUDTF Example: Extracted Hashtags")
spark_session.sql("SELECT * FROM extract_hashtags('Welcome to #ApacheSpark and #BigData!')").show()

# ----------------------------
# Example usage of UDF in Spark SQL
# ----------------------------
print("\nUDF Example: Count Hashtags")
spark_session.sql(
    "SELECT count_hashtags('Welcome to #ApacheSpark and #BigData!') AS hashtag_count"
).show()

# ----------------------------
# Using both UDTF and UDF with DataFrame operations
# ----------------------------
sample_data = [("Learning #AI with #ML",), ("Explore #DataScience",), ("No hashtags here",)]
tweets_df = spark_session.createDataFrame(sample_data, ["text"])

# Apply UDF to count hashtags per row
tweets_df.selectExpr("text", "count_hashtags(text) AS num_hashtags").show()

# Apply UDTF with LATERAL JOIN in SQL to explode hashtags per row
print("\nUDTF with LATERAL JOIN Example:")
tweets_df.createOrReplaceTempView("tweets")
spark_session.sql(
    "SELECT text, hashtag FROM tweets, LATERAL extract_hashtags(text)"
).show()

# Stop Spark session
spark_session.stop()
