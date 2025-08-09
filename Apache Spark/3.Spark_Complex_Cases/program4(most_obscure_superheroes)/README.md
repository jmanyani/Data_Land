### README: Finding the Most Obscure Marvel Superheroes with PySpark



This script performs an analysis on a superhero co-appearance graph to find characters with the least number of connections, i.e., the most obscure ones. It covers:



* Reading superhero IDs and names into a structured DataFrame
* Parsing raw graph data to compute how many other superheroes each one has appeared with
* Identifying the minimum connection count across all characters
* Filtering out the superheroes who share this minimum number of connections
* Joining with the names dataset to display actual character names



**Requirements**



* Python 3.x
* PySpark



**Dataset files:**



* Marvel+names.txt (contains ID and name mappings)
* Marvel+graph.txt (contains co-appearance data in raw text format)



**Key Concepts**



* Text Preprocessing: Using split, trim, and size to clean and interpret graph data
* Aggregation Functions: groupBy + sum to count connections, and agg + min to find the least connected
* Filtering: Extracting only those records that match the minimum connection count
* Join Operations: Combining connection data with character names using a common ID
* DataFrame Display: .show() used to list all obscure characters in tabular form
