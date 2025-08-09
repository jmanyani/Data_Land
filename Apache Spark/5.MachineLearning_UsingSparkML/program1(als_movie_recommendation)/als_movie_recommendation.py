from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs

def load_movie_names(file_path):
    movie_names = {}
    with codecs.open(file_path, "r", encoding="ISO-8859-1", errors="ignore") as file:
        for line in file:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names

def main(user_id):
    spark = SparkSession.builder.appName("ALSRecommendation").getOrCreate()

    schema = StructType([
        StructField("userID", IntegerType(), True),
        StructField("movieID", IntegerType(), True),
        StructField("rating", IntegerType(), True),
        StructField("timestamp", LongType(), True)
    ])

    movie_names = load_movie_names("c://SparkCourse/ml-100k/u.ITEM")

    ratings = spark.read.option("sep", "\t").schema(schema).csv("c://SparkCourse/ml-100k/u.data")

    print("Training ALS recommendation model...")
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating", coldStartStrategy="drop")
    model = als.fit(ratings)

    user_schema = StructType([StructField("userID", IntegerType(), True)])
    users_df = spark.createDataFrame([[user_id]], user_schema)

    recommendations = model.recommendForUserSubset(users_df, 10).collect()

    print(f"Top 10 movie recommendations for user ID {user_id}:")
    for user_rec in recommendations:
        recs = user_rec[1]  # List of recommendations for user
        for rec in recs:
            movie_id = rec.movieID
            predicted_rating = rec.rating
            movie_title = movie_names.get(movie_id, "Unknown Movie")
            print(f"{movie_title}: {predicted_rating:.2f}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit script.py <user_id>")
        sys.exit(1)
    user_id = int(sys.argv[1])
    main(user_id)
