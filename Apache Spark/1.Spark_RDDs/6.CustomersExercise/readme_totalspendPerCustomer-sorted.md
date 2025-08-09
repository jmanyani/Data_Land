**📘 README : Total Spend by Customer (Sorted)**



**Description:**

This PySpark script reads customer purchase data, calculates total spending per customer, and then sorts the results by total amount spent in ascending order. It demonstrates mapping, aggregation, key flipping, and sorting operations in Spark.



**Steps Performed:**



1. Read from customer-orders.csv.
   
2. Extract customer ID and purchase amount.
   
3. Aggregate total spend per customer using reduceByKey.
   
4. Flip (customer, total) → (total, customer) for sorting.
   
5. Sort by total amount spent and print results.



**Requirements:**



* Apache Spark



* Python 3



* Dataset: customer-orders.csv
