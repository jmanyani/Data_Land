# This Spark program calculates the average number of friends for each age
# based on a social network dataset.

from pyspark import SparkConf, SparkContext

# Configure Spark for local execution
spark_config = SparkConf().setMaster("local").setAppName("AverageFriendsByAge")

# Initialize SparkContext
spark_context = SparkContext(conf=spark_config)

# Function to parse each line of the CSV into (age, numFriends)
def extract_age_friends(record):
    columns = record.split(",")
    age = int(columns[2])
    friends_count = int(columns[3])
    return (age, friends_count)

# Load dataset from file (placeholder path used here)
social_data = spark_context.textFile("XXXXX")

# Transform data into (age, numFriends) pairs
age_friend_pairs = social_data.map(extract_age_friends)

# Convert to (age, (totalFriends, count)) and aggregate totals
age_friend_totals = age_friend_pairs.mapValues(lambda count: (count, 1)) \
                                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Compute average number of friends per age
average_friends_per_age = age_friend_totals.mapValues(lambda totals: totals[0] / totals[1])

# Collect results into a list
final_results = average_friends_per_age.collect()

# Display results
for entry in final_results:
    print(entry)
