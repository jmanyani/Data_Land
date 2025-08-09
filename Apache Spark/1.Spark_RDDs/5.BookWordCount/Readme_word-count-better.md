**📘 README : Normalized Word Count**



**Description:**

This PySpark script reads a text file, normalizes the words by converting to lowercase and splitting on non-word characters (punctuation, spaces), and then counts how often each word appears. It also filters out non-ASCII words to keep the output clean.



**Steps Performed:**



1. Read the input text file.
2. Normalize words using regex to split on non-word characters and lowercase all text.
3. Count occurrences of each normalized word with countByValue.
4. Filter out non-ASCII words.
5. Print the results.



**Requirements:**



* Apache Spark
* Python 3
* Text File: book.txt
