### README: Marvel Superhero Popularity Analysis with PySpark



This script identifies the most popular Marvel superhero based on how often they appear alongside others in comic issues. It demonstrates:



* Reading superhero names and their connection graph using PySpark
* Parsing and cleaning graph data using split, trim, and size
* Calculating the number of co-appearances for each superhero
* Joining superhero names with their IDs to find the most popular character
* Displaying the superhero with the highest number of connections



**Requirements**



* Python 3.x
* PySpark



**Dataset files:**



* Marvel+names.txt containing ID-to-name mappings
* Marvel+graph.txt containing superhero co-appearance data



**Key Concepts**



* Text Parsing with DataFrames: Using Spark functions like split, trim, and size to process raw text lines
* Aggregation: Summing co-appearances using groupBy and agg
* Joins via Filtering: Matching superhero IDs to their names
* Efficiency: Uses .first() to retrieve only the top result for performance
