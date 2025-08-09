### README: Degrees of Separation Between Marvel Superheroes Using PySpark (BFS Algorithm)



This script uses PySpark to compute the shortest path (degrees of separation) between two superheroes in the Marvel universe. It applies a breadth-first search (BFS) algorithm across a large graph dataset of character co-appearances.



**It covers:**

* Initializing a Spark context and defining the source (SpiderMan) and target (ADAM 3,031) character IDs
* Creating a BFS-friendly RDD structure with distance and color (WHITE, GRAY, BLACK) to track traversal state
* Iteratively expanding nodes marked GRAY to explore their neighbors
* Using a Spark accumulator to detect when the target character is found
* Using custom map and reduce functions (bfsMap, bfsReduce) to simulate BFS traversal over distributed data



**Requirements**

* Python 3.x
* PySpark
* Marvel graph file: marvel+graph.txt placed in c://sparkcourse/



**Key Concepts**



* Breadth-First Search (BFS): A common algorithm for finding shortest paths in unweighted graphs
* Graph Traversal in Spark: Simulating traversal using iterative RDD transformations
* Accumulators: Shared counters across tasks, used here to detect target character discovery
* Custom Reducers: To merge paths and preserve shortest distance and traversal state





**Colors in BFS:**



* WHITE = Unvisited
* GRAY = Visited but not fully explored
* BLACK = Fully explored











