**📘 README for Program: Average Number of Friends by Age**



**Description:**

This PySpark script reads a CSV dataset of fake social network users and computes the average number of friends grouped by age. It uses basic RDD transformations like map, reduceByKey, and mapValues.



**Steps Performed:**



1. Load and parse each line from the dataset.
   
2. Extract age and number of friends.
   
3. Sum total friends and count entries for each age.
   
4. Calculate average number of friends per age group.
   
5. Print the results.



**Requirements:**



* Apache Spark
* Python 3
* Dataset: fakefriends-no-header.csv
