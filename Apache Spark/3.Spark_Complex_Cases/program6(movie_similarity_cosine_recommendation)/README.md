### README: Movie Recommendations with Cosine Similarity using PySpark



This script analyzes user rating data to find and recommend similar movies based on cosine similarity, a mathematical metric that compares rating patterns between movie pairs. It simulates collaborative filtering by matching how users rate different movie combinations.



**It covers:**

* Reading movie metadata and rating data from the MovieLens dataset
* Using a self-join on user ratings to create all possible movie pairs rated by the same user
* Computing similarity scores using the cosine similarity formula
* Caching the similarity matrix for performance
* Filtering recommendations based on configurable score and co-occurrence thresholds
* Displaying the top 10 most similar movies for a given movie ID



**Requirements**

* Python 3.x
* PySpark
* MovieLens 100k dataset:
* u.item (movie ID and title, pipe-separated)
* u.data (user ratings, tab-separated)
* Run the script with a movie ID as a command-line argument (e.g., python movie\_similarity.py 50)



**Key Concepts**

* Cosine Similarity: Measures the angle between two rating vectors; higher score means more similar
* Self-Join on User Ratings: Creates combinations of all movies rated together by the same user
* Pairwise Aggregation: For each movie pair, computes:



1. Dot product of ratings (xy)
2. Squared magnitude of ratings (xx and yy)
3. Similarity score = xy / sqrt(xx \* yy)





* Threshold Filtering: Avoids noisy results by ignoring pairs with low similarity or low number of co-ratings
* 
* Broadcast Joins for Metadata Lookup: Matches movie IDs with names to display readable output
