# This Spark program counts normalized words in a text file
# and sorts the output by word frequency in ascending order.

import re
from pyspark import SparkConf, SparkContext

# Configure Spark for local execution
spark_conf = SparkConf().setMaster("local").setAppName("SortedWordCount")

# Initialize SparkContext
spark_context = SparkContext(conf=spark_conf)

# Function to extract words by matching word characters, converting to lowercase
def extract_words(text):
    return re.compile(r'\w+', re.UNICODE).findall(text.lower())

# Load input text file (placeholder path used)
text_rdd = spark_context.textFile("XXXXX")

# Split lines into words
words_rdd = text_rdd.flatMap(extract_words)

# Create (word, 1) pairs and sum counts by word
word_counts = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Flip to (count, word) and sort by count ascending
sorted_word_counts = word_counts.map(lambda pair: (pair[1], pair[0])).sortByKey()

# Collect results to driver
results = sorted_word_counts.collect()

# Print sorted word frequencies
for count, word in results:
    print(f"{word}\t\t |\t\t {count}")
