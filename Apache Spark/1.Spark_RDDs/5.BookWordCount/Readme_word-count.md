**📘 README for Program : Word Count from Text File**



**Description:**

This PySpark script reads a plain text file (e.g., a book), splits it into words, and counts how often each word appears. Non-ASCII characters are ignored to avoid encoding issues. It demonstrates the use of flatMap and countByValue in PySpark.



**Steps Performed:**



1. Read the input text file.
   
2. Split each line into individual words.
   
3. Count occurrences of each word using countByValue.
   
4. Filter out non-ASCII words.
   
5. Print the word counts.



**Requirements:**



* Apache Spark
* Python 3
* Text File: book.txt (can be any large text document)
