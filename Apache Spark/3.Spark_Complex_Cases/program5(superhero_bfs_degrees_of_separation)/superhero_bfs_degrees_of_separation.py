from pyspark import SparkConf, SparkContext

# Initialize Spark context for local mode
conf = SparkConf().setMaster("local").setAppName("SuperheroDegreesOfSeparation")
sc = SparkContext(conf=conf)

# Start and target superhero IDs
start_hero_id = 5306  # SpiderMan
target_hero_id = 14   # ADAM 3,031 (example target)

# Accumulator to detect when target hero is found during BFS
found_target_acc = sc.accumulator(0)

def parse_line_to_bfs(line):
    parts = line.split()
    hero_id = int(parts[0])
    neighbors = list(map(int, parts[1:]))

    color = 'WHITE'
    distance = 9999

    if hero_id == start_hero_id:
        color = 'GRAY'
        distance = 0

    return (hero_id, (neighbors, distance, color))

def initialize_bfs_rdd():
    raw_data = sc.textFile("XXXXX")
    return raw_data.map(parse_line_to_bfs)

def bfs_expand(node):
    hero_id, (neighbors, dist, color) = node
    results = []

    if color == 'GRAY':
        for nbr in neighbors:
            new_dist = dist + 1
            new_color = 'GRAY'
            if nbr == target_hero_id:
                found_target_acc.add(1)
            results.append((nbr, ([], new_dist, new_color)))
        color = 'BLACK'  # Mark this node as processed

    # Emit the current node data
    results.append((hero_id, (neighbors, dist, color)))
    return results

def bfs_combine(data1, data2):
    nbrs1, dist1, color1 = data1
    nbrs2, dist2, color2 = data2

    neighbors = nbrs1 if len(nbrs1) > 0 else nbrs2
    if len(nbrs2) > 0 and len(nbrs1) > 0:
        neighbors = nbrs1 + [n for n in nbrs2 if n not in nbrs1]

    distance = min(dist1, dist2)

    # Color priority: WHITE < GRAY < BLACK
    colors = ['WHITE', 'GRAY', 'BLACK']
    color = color1 if colors.index(color1) > colors.index(color2) else color2

    return (neighbors, distance, color)

# Initialize RDD for BFS traversal
bfs_rdd = initialize_bfs_rdd()

# Limit iterations to avoid infinite loops
for iteration in range(10):
    print(f"Running BFS iteration #{iteration + 1}")

    # Expand frontier nodes
    mapped = bfs_rdd.flatMap(bfs_expand)

    # Force evaluation and update accumulator
    count = mapped.count()
    print(f"Processed {count} nodes.")

    if found_target_acc.value > 0:
        print(f"Found target hero! Hit count: {found_target_acc.value}")
        break

    # Combine info from duplicate nodes after expansion
    bfs_rdd = mapped.reduceByKey(bfs_combine)
