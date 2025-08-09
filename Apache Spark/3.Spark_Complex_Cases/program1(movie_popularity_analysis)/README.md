### README: Popular Movies Analysis with PySpark



* This script demonstrates how to analyze movie rating data using PySpark. It covers:
* Initializing a Spark session
* Defining a schema for structured reading of raw tab-separated data
* Loading the movie rating dataset into a DataFrame
* Using groupBy and count to find the popularity of movies
* Sorting and displaying the top 10 most popular movies based on rating counts



**Requirements**



* Python 3.x
* PySpark
* The MovieLens dataset file u.data placed at c://SparkCourse/ml-100k/u.data



**Key Concepts**



* Defining a schema with StructType and StructField for structured data loading
* Using DataFrame transformations: groupBy(), count(), and orderBy() for aggregation and sorting
* SparkSession as the entry point for Spark functionality
* Using .show() to display results in tabular format
