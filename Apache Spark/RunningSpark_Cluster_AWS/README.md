### README: Movie Similarity Computation with PySpark on MovieLens 1M



This script implements collaborative filtering by calculating pairwise movie similarity using cosine similarity on the MovieLens 1M dataset. It identifies the top similar movies for a given movie ID based on user rating patterns.



#### What this script covers:

* Loading movie metadata for display purposes (movies.dat)
* Reading user ratings data (ratings.dat) from S3
* Mapping user ratings to key-value pairs (userID -> (movieID, rating))
* Generating all pairs of movies rated by the same user using a self-join
* Filtering duplicate pairs to avoid redundancy
* Grouping ratings by movie pairs and computing cosine similarity as a measure of similarity
* Persisting similarity results to storage for further use
* Filtering and displaying top similar movies for a specific movie based on thresholds for similarity score and co-occurrence count



**Usage**:

spark-submit --executor-memory 1g MoviesSimilarities1M.py <movieID>





**Key Concepts:**

1. Cosine Similarity is used to measure similarity between two movies based on user rating vectors
2. RDD joins and transformations are employed for efficient pairwise computations
3. Filtering duplicates avoids redundant movie pair calculations
4. Persistence and partitioning optimize performance and scalability for large datasets
5. Thresholds on similarity and co-occurrence ensure recommendations are meaningful
