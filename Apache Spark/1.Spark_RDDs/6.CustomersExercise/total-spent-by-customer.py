# This Spark program calculates the total spending for each customer
# from a sales dataset.

from pyspark import SparkConf, SparkContext

# Configure Spark for local use
spark_conf = SparkConf().setMaster("local").setAppName("CustomerTotalSpending")

# Initialize SparkContext
spark_context = SparkContext(conf=spark_conf)

# Function to parse each line into (customerID, purchaseAmount)
def parse_customer_spending(record):
    fields = record.split(',')
    customer_id = int(fields[0])
    amount_spent = float(fields[2])
    return (customer_id, amount_spent)

# Load sales data (placeholder path used)
sales_rdd = spark_context.textFile("XXXXX")

# Map each record to (customerID, amountSpent)
customer_spending_pairs = sales_rdd.map(parse_customer_spending)

# Sum all amounts by customerID
total_spent_per_customer = customer_spending_pairs.reduceByKey(lambda a, b: a + b)

# Collect and display results
results = total_spent_per_customer.collect()
for customer, total in results:
    print(f"Customer {customer}: ${total:.2f}")
