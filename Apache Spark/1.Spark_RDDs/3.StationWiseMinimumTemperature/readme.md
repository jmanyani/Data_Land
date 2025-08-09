**📘 README for Program : Minimum Temperatures by Station**



**Description:**

This PySpark script analyzes a weather dataset and finds the lowest recorded temperature for each weather station. It parses a CSV file, filters for minimum temperature records (TMIN), and uses reduceByKey to compute the minimum temperature per station.



**Steps Performed:**



1. Load and parse the weather data from a CSV file.
   
2. Filter only temperature entries with type TMIN.
   
3. Convert temperatures to Fahrenheit.
   
4. Find the minimum temperature for each station using reduceByKey.
   
5. Print the results.



**Requirements:**



* Apache Spark
* Python 3
* Dataset: 1800.csv (from course resources)
