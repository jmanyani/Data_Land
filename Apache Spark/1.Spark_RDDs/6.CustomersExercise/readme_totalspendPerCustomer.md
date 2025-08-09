**README for Program 8: Total Spend by Customer**



**Description:**

This PySpark script processes a CSV file containing customer purchase records and calculates the total amount spent by each customer. It showcases basic transformations like map and reduceByKey for aggregation tasks.



**Steps Performed:**



1. Read purchase data from customer-orders.csv.
   
2. Extract customer ID and purchase amount.
   
3. Sum total spend per customer using reduceByKey.
   
4. Print each customer’s total spend.



**Requirements:**



* Apache Spark
* Python 3
* Dataset: customer-orders.csv (provided in course files)
