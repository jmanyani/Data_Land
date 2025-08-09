import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

def load_movie_titles():
    movie_titles = {}
    with open("############################", encoding='ascii', errors='ignore') as file:
        for line in file:
            fields = line.split("::")
            movie_titles[int(fields[0])] = fields[1]
    return movie_titles

def create_movie_pair(user_ratings):
    ratings = user_ratings[1]
    (movie_a, rating_a) = ratings[0]
    (movie_b, rating_b) = ratings[1]
    return ((movie_a, movie_b), (rating_a, rating_b))

def remove_duplicate_pairs(user_ratings):
    ratings = user_ratings[1]
    (movie_a, rating_a) = ratings[0]
    (movie_b, rating_b) = ratings[1]
    return movie_a < movie_b  # keep only one order of pair

def cosine_similarity(rating_pairs):
    count = 0
    sum_xx = sum_yy = sum_xy = 0
    for rating_x, rating_y in rating_pairs:
        sum_xx += rating_x * rating_x
        sum_yy += rating_y * rating_y
        sum_xy += rating_x * rating_y
        count += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    similarity_score = 0
    if denominator != 0:
        similarity_score = numerator / float(denominator)

    return (similarity_score, count)


# Initialize Spark context
spark_conf = SparkConf()
spark_context = SparkContext(conf=spark_conf)

print("\nLoading movie titles...")
movies_dict = load_movie_titles()

# Load ratings data from S3 bucket
ratings_raw = spark_context.textFile("s3://moviesdatafilefolder/ratings.dat")

# Parse ratings into (userID, (movieID, rating)) tuples
user_ratings = ratings_raw.map(lambda line: line.split("::")) \
                          .map(lambda parts: (int(parts[0]), (int(parts[1]), float(parts[2]))))

# Partition ratings for efficient joins
partitioned_ratings = user_ratings.partitionBy(100)

# Self-join to find all movie pairs rated by same user
joined_ratings = partitioned_ratings.join(partitioned_ratings)

# Filter out duplicate movie pairs (keep only movie1 < movie2)
unique_pairs = joined_ratings.filter(remove_duplicate_pairs)

# Create key-value pairs ((movie1, movie2), (rating1, rating2))
movie_rating_pairs = unique_pairs.map(create_movie_pair).partitionBy(100)

# Group all ratings for each movie pair
grouped_ratings = movie_rating_pairs.groupByKey()

# Compute cosine similarity for each movie pair
movie_similarity_scores = grouped_ratings.mapValues(cosine_similarity).persist()

# Save results if needed
movie_similarity_scores.sortByKey().saveAsTextFile("movie-sims")

# If movie ID provided as argument, find top similar movies above thresholds
if len(sys.argv) > 1:
    target_movie_id = int(sys.argv[1])
    similarity_threshold = 0.97
    co_occurrence_threshold = 50

    # Filter to keep only similar movies meeting thresholds
    filtered_similarities = movie_similarity_scores.filter(lambda pair_sim:
        (pair_sim[0][0] == target_movie_id or pair_sim[0][1] == target_movie_id) and
        pair_sim[1][0] > similarity_threshold and
        pair_sim[1][1] > co_occurrence_threshold)

    # Sort by similarity score descending and take top 10
    top_similar_movies = filtered_similarities.map(lambda x: (x[1], x[0])) \
                                             .sortByKey(ascending=False) \
                                             .take(10)

    print(f"Top 10 similar movies for {movies_dict[target_movie_id]}:")
    for sim_score, movie_pair in top_similar_movies:
        similar_movie_id = movie_pair[0] if movie_pair[0] != target_movie_id else movie_pair[1]
        print(f"{movies_dict[similar_movie_id]}\tscore: {sim_score[0]:.4f}\tstrength: {sim_score[1]}")
