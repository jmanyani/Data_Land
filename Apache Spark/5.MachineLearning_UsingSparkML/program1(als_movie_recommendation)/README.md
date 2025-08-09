### README: Personalized Movie Recommendations with ALS in PySpark



This script implements a collaborative filtering recommendation system using the ALS (Alternating Least Squares) algorithm from Spark MLlib. It predicts user preferences based on historical ratings.



**It covers:**

* Loading movie rating data and movie names from the MovieLens dataset
* Training an ALS model with configured iterations and regularization
* Creating a DataFrame for a specific user ID to get personalized recommendations
* Generating and displaying the top 10 recommended movies with predicted ratings for that user



**Requirements**

* Python 3.x
* PySpark with MLlib
* MovieLens 100k dataset files:
* u.data (ratings data)
* u.ITEM (movie names)
* Run the script with a user ID argument (e.g., python als\_recommendation.py 123)



**Key Concepts**

* ALS Algorithm: A matrix factorization technique well-suited for collaborative filtering
* Spark MLlib: Spark’s scalable machine learning library
* DataFrames \& Schema: Structured input with user, movie, rating, and timestamp columns
* Model Training: Using .fit() to train the ALS model on user ratings
* Recommendation Generation: Using recommendForUserSubset() to get top N movie suggestions
* Broadcast Variables: Loading movie names into a Python dictionary for readable output
