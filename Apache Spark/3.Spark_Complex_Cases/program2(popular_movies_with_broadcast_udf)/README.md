### README: Displaying Most Popular Movies with Titles using PySpark



This script builds on basic popularity analysis by enriching movie rating data with movie titles. It covers:



* Loading movie names from the u.ITEM file into a Python dictionary
* Broadcasting that dictionary to all Spark executors
* Reading movie ratings data using a defined schema
* Counting the number of ratings per movie ID
* Using a User-Defined Function (UDF) to map movie IDs to titles
* Adding a movie title column to the DataFrame
* Sorting and displaying the top 10 most popular movies by rating count



**Requirements**



* Python 3.x
* PySpark
* The following files in your project directory:
* u.data (rating data)
* u.ITEM (movie ID-to-name mapping)



**Key Concepts**



* Broadcast Variables: Efficiently share read-only data (like lookup dictionaries) across Spark workers
* UDF (User Defined Function): Custom function to transform columns using Python logic
* DataFrame Transformations: groupBy, count, withColumn, and orderBy
* Structured Data Reading: Schema-defined parsing of tab-separated rating files
