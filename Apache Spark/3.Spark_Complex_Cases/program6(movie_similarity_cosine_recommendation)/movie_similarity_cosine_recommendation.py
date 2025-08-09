from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys

def computeCosineSimilarity(data):
    # Calculate intermediate columns for cosine similarity
    pairScores = data \
      .withColumn("xx", func.col("rating1") * func.col("rating1")) \
      .withColumn("yy", func.col("rating2") * func.col("rating2")) \
      .withColumn("xy", func.col("rating1") * func.col("rating2"))

    # Aggregate sums to calculate numerator and denominator for cosine similarity
    similarity = pairScores.groupBy("movie1", "movie2").agg(
        func.sum("xy").alias("numerator"),
        (func.sqrt(func.sum("xx")) * func.sqrt(func.sum("yy"))).alias("denominator"),
        func.count("xy").alias("numPairs")
    )

    # Calculate cosine similarity score, avoid division by zero
    result = similarity.withColumn("score",
            func.when(func.col("denominator") != 0,
                      func.col("numerator") / func.col("denominator"))
                .otherwise(0)
        ).select("movie1", "movie2", "score", "numPairs")

    return result

def getMovieName(movieNamesDF, movieId):
    name = movieNamesDF.filter(func.col("movieID") == movieId).select("movieTitle").collect()
    if name:
        return name[0][0]
    else:
        return "Unknown"

spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([
    StructField("movieID", IntegerType(), True),
    StructField("movieTitle", StringType(), True)
])

moviesSchema = StructType([
    StructField("userId", IntegerType(), True),
    StructField("movieId", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

movieNames = spark.read.option("sep", "|") \
    .option("charset", "ISO-8859-1") \
    .schema(movieNamesSchema) \
    .csv("c://SparkCourse/ml-100k/u.item")

movies = spark.read.option("sep", "\t") \
    .schema(moviesSchema) \
    .csv("c://SparkCourse/ml-100k/u.data")

ratings = movies.select("userId", "movieId", "rating")

# Generate all pairs of movies rated by the same user (movie1 < movie2 to avoid duplicates)
moviePairs = ratings.alias("r1").join(
    ratings.alias("r2"),
    (func.col("r1.userId") == func.col("r2.userId")) &
    (func.col("r1.movieId") < func.col("r2.movieId"))
).select(
    func.col("r1.movieId").alias("movie1"),
    func.col("r2.movieId").alias("movie2"),
    func.col("r1.rating").alias("rating1"),
    func.col("r2.rating").alias("rating2")
)

moviePairSimilarities = computeCosineSimilarity(moviePairs).cache()

if len(sys.argv) > 1:
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movieID = int(sys.argv[1])

    filteredResults = moviePairSimilarities.filter(
        ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &
        (func.col("score") > scoreThreshold) &
        (func.col("numPairs") > coOccurrenceThreshold)
    )

    results = filteredResults.sort(func.col("score").desc()).take(10)

    print(f"Top 10 similar movies for {getMovieName(movieNames, movieID)}:")

    for result in results:
        similarMovieID = result.movie1
        if similarMovieID == movieID:
            similarMovieID = result.movie2
        print(f"{getMovieName(movieNames, similarMovieID)}\tscore: {result.score:.4f}\tstrength: {result.numPairs}")

spark.stop()
