from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize Spark session
spark_session = SparkSession.builder.appName("MostObscureSuperheroesAnalysis").getOrCreate()

# Schema for superhero id and name
hero_schema = StructType([
    StructField("hero_id", IntegerType(), True),
    StructField("hero_name", StringType(), True)
])

# Load superhero names
hero_names_df = spark_session.read.schema(hero_schema).option("sep", " ").csv("XXXXX")

# Load superhero connections graph (each line lists one hero's connections)
graph_lines_df = spark_session.read.text("XXXXX")

# Compute number of connections per hero by parsing graph lines
connections_df = graph_lines_df \
    .withColumn("hero_id", F.split(F.trim(F.col("value")), " ")[0]) \
    .withColumn("connection_count", F.size(F.split(F.trim(F.col("value")), " ")) - 1) \
    .groupBy("hero_id").agg(F.sum("connection_count").alias("total_connections"))

# Find minimum number of connections across all heroes
min_connections_count = connections_df.agg(F.min("total_connections")).first()[0]

# Filter heroes with minimum connections
obscure_heroes_df = connections_df.filter(F.col("total_connections") == min_connections_count)

# Join with hero names to get readable names
obscure_heroes_with_names_df = obscure_heroes_df.join(hero_names_df, "hero_id")

print(f"The following characters have only {min_connections_count} connection(s):")

# Display names of most obscure heroes
obscure_heroes_with_names_df.select("hero_name").show(truncate=False)

# Stop Spark session
spark_session.stop()
