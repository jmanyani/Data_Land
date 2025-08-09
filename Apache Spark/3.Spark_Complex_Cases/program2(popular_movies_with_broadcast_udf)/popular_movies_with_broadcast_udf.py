from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

# Broadcast dictionary loader for movie ID to movie name mapping
def load_movie_names():
    movie_names = {}
    # Replace with your actual path or mask for privacy
    with codecs.open("XXXXX", "r", encoding='ISO-8859-1', errors='ignore') as file:
        for line in file:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names

# Initialize Spark session
spark_session = SparkSession.builder.appName("PopularMoviesWithNames").getOrCreate()

# Broadcast movie names dictionary to executors
broadcast_movie_names = spark_session.sparkContext.broadcast(load_movie_names())

# Define schema for ratings data
ratings_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

# Load movie ratings data
ratings_df = spark_session.read.option("sep", "\t").schema(ratings_schema).csv("XXXXX")

# Count ratings per movie
movie_counts_df = ratings_df.groupBy("movie_id").count()

# Define UDF to lookup movie name from broadcast dictionary
def get_movie_name(movie_id):
    return broadcast_movie_names.value.get(movie_id, "Unknown")

lookup_name_udf = F.udf(get_movie_name)

# Add movieTitle column by applying UDF
movies_with_names_df = movie_counts_df.withColumn("movieTitle", lookup_name_udf(F.col("movie_id")))

# Sort by popularity descending
sorted_movies_df = movies_with_names_df.orderBy(F.desc("count"))

# Show top 10 movies with names
sorted_movies_df.show(10, truncate=False)

# Stop Spark session
spark_session.stop()
