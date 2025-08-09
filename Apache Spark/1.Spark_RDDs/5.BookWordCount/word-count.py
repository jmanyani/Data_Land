# This Spark program counts how often each word appears in a text file.

from pyspark import SparkConf, SparkContext

# Set up Spark to run locally with a specific app name
spark_conf = SparkConf().setMaster("local").setAppName("WordFrequencyCount")

# Initialize SparkContext
spark_context = SparkContext(conf=spark_conf)

# Load the input text file (placeholder path used)
text_rdd = spark_context.textFile("XXXXX")

# Split each line into words
all_words = text_rdd.flatMap(lambda line: line.split())

# Count the occurrences of each word
word_counts = all_words.countByValue()

# Print each word and its count, ignoring non-ASCII characters
for word, count in word_counts.items():
    ascii_word = word.encode("ascii", "ignore")
    if ascii_word:
        print(ascii_word.decode() + " " + str(count))
