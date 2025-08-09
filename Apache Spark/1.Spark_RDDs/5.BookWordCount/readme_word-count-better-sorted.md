**README : Sorted Word Count with Normalization**



**Description:**

This PySpark script reads a text file, normalizes the words (lowercasing and extracting only alphanumeric sequences), counts how many times each word occurs, and then sorts the results by frequency. It uses transformations like flatMap, map, reduceByKey, and sortByKey.



**Steps Performed:**



1. Read and normalize the input text (lowercase and extract words using regex).
   
2. Map each word to a (word, 1) pair.
   
3. Use reduceByKey to get total word counts.
   
4. Flip the key-value pair to (count, word) for sorting.
   
5. Collect and print results sorted by count.



**Requirements:**



* Apache Spark



* Python 3



* Dataset: book.txt
