# This Spark program counts word frequencies from a text file,
# normalizing words by converting to lowercase and splitting on non-word characters.

import re
from pyspark import SparkConf, SparkContext

# Function to normalize and split text into words
def normalize_text(line):
    return re.compile(r'\W+', re.UNICODE).split(line.lower())

# Configure Spark for local execution
spark_config = SparkConf().setMaster("local").setAppName("NormalizedWordCount")

# Initialize SparkContext
spark_context = SparkContext(conf=spark_config)

# Load input text file (placeholder path used)
text_rdd = spark_context.textFile("XXXXX")

# Apply normalization and split into words
normalized_words = text_rdd.flatMap(normalize_text)

# Count occurrences of each normalized word
word_frequencies = normalized_words.countByValue()

# Print each ASCII word and its count
for word, count in word_frequencies.items():
    ascii_word = word.encode('ascii', 'ignore')
    if ascii_word:
        print(ascii_word.decode() + " " + str(count))
