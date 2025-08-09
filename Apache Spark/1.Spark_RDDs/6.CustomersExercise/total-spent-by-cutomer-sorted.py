# This Spark program calculates total spending by each customer,
# then sorts customers by their spending amount in ascending order.

from pyspark import SparkConf, SparkContext

# Configure Spark for local execution
spark_conf = SparkConf().setMaster("local").setAppName("SortedSpendByCustomer")

# Initialize SparkContext
spark_context = SparkContext(conf=spark_conf)

# Parse each line into (customerID, amountSpent)
def parse_customer_amount(record):
    fields = record.split(',')
    customer_id = int(fields[0])
    amount = float(fields[2])
    return (customer_id, amount)

# Load sales data (placeholder path used)
sales_data = spark_context.textFile("XXXXX")

# Map records to (customerID, amountSpent)
customer_amount_pairs = sales_data.map(parse_customer_amount)

# Aggregate total spending by customer
total_spent = customer_amount_pairs.reduceByKey(lambda a, b: a + b)

# Flip to (totalSpent, customerID) for sorting
flipped_total_spent = total_spent.map(lambda x: (x[1], x[0]))

# Sort by total spending (ascending)
sorted_spending = flipped_total_spent.sortByKey()

# Collect and print results
for amount, customer in sorted_spending.collect():
    print(f"Customer {customer}: ${amount:.2f}")
