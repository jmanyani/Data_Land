from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

# Initialize Spark session
spark_session = SparkSession.builder.appName("MoviePopularityAnalysis").getOrCreate()

# Define schema for MovieLens ratings data
ratings_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Load movie ratings data with specified schema and tab delimiter
ratings_df = spark_session.read.option("sep", "\t").schema(ratings_schema).csv("XXXXX")

# Group by movie_id and count ratings to determine popularity, then sort descending
movie_popularity_df = ratings_df.groupBy("movie_id").count().orderBy(F.desc("count"))

# Show top 10 most popular movies
movie_popularity_df.show(10)

# Stop Spark session
spark_session.stop()
