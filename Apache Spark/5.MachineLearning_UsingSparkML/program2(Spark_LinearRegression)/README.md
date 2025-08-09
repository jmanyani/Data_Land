### README: Simple Linear Regression with PySpark MLlib



This script demonstrates how to build, train, and evaluate a linear regression model using Spark's MLlib. It’s a minimal example meant to show the basics of fitting a regression line to input data and generating predictions.



**It covers:**

* Creating a Spark session
* Loading and parsing a CSV-style text dataset (regression.txt) into labeled feature vectors
* Converting RDD data to a DataFrame with "label" and "features" columns
* Splitting data randomly into training and test sets
* Training a Linear Regression model using Spark MLlib
* Applying the model to test data to generate predictions
* Comparing actual vs. predicted values



**Requirements**

* Python 3.x
* PySpark
* A text file regression.txt formatted as:



label,feature  

3.5,1.0  

4.0,2.0  

...  



**Key Concepts**

* Linear Regression: A supervised learning method to predict a numeric value based on input features
* Vectors.dense: Used to format feature columns for MLlib models
* Data Splitting: randomSplit() divides data into training and testing portions
* Model Training \& Prediction: fit() trains the model, and transform() applies it to new data
* Model Evaluation: Actual vs. predicted values are printed for review
