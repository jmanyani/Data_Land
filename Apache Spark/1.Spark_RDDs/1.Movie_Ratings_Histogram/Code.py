# This Spark program reads the MovieLens 100k dataset
# and produces a histogram showing the distribution of movie ratings.

from pyspark import SparkConf, SparkContext
import collections

# Configure Spark to run locally and set the application name
spark_config = SparkConf().setMaster("local").setAppName("MovieLens Ratings Histogram")

# Initialize the SparkContext
spark_context = SparkContext(conf=spark_config)

# Load the MovieLens data file into an RDD (placeholder path used here)
ratings_data = spark_context.textFile("XXXXX")

# Extract only the rating value (3rd column) from each line
rating_values = ratings_data.map(lambda record: record.split()[2])

# Count how many times each rating value occurs
rating_counts = rating_values.countByValue()

# Sort the ratings in ascending order for nicer display
sorted_rating_counts = collections.OrderedDict(sorted(rating_counts.items()))

# Display each rating with its corresponding count
for rating, count in sorted_rating_counts.items():
    print(f"{rating} {count}")
