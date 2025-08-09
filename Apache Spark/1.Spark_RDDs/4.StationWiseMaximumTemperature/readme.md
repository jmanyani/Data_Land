**📘 README for Program 4: Maximum Temperatures by Station**



**Description:**

This PySpark script processes a historical weather dataset to determine the highest temperature recorded at each weather station. It filters records marked with the TMAX entry type, converts temperatures from tenths of Celsius to Fahrenheit, and finds the maximum per station.



**Steps Performed:**



1. Load and parse weather data from a CSV file.
   
2. Filter records with type TMAX (maximum temperature).
   
3. Convert temperatures to Fahrenheit.
   
4. Use reduceByKey to find the maximum temperature per station.
   
5. Print results in a formatted output.



**Requirements:**



* Apache Spark



* Python 3



* Dataset: 1800.csv (from the course dataset)
