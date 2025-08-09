from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark_session = SparkSession.builder.appName("MostPopularSuperheroAnalysis").getOrCreate()

# Define schema for superhero ID and name
names_schema = StructType([
    StructField("hero_id", IntegerType(), True),
    StructField("hero_name", StringType(), True)
])

# Load superhero names
hero_names_df = spark_session.read.schema(names_schema).option("sep", " ").csv("XXXXX")

# Load graph data: each line represents connections for one hero
graph_lines_df = spark_session.read.text("XXXXX")

# Parse graph lines to get number of connections per hero
connections_df = graph_lines_df \
    .withColumn("hero_id", F.split(F.trim(F.col("value")), " ")[0]) \
    .withColumn("connections_count", F.size(F.split(F.trim(F.col("value")), " ")) - 1) \
    .groupBy("hero_id").agg(F.sum("connections_count").alias("total_connections"))

# Get the hero with the maximum connections (most popular)
most_popular_hero = connections_df.orderBy(F.col("total_connections").desc()).first()

# Lookup hero name by hero_id
popular_hero_name_row = hero_names_df.filter(F.col("hero_id") == int(most_popular_hero["hero_id"])).select("hero_name").first()

print(f"{popular_hero_name_row['hero_name']} is the most popular superhero with {most_popular_hero['total_connections']} co-appearances.")

# Stop Spark session
spark_session.stop()
