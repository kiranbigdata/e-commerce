from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("ECommercePipeline").getOrCreate()

# Load CSV Files
customers_df = spark.read.csv("customers.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)
order_items_df = spark.read.csv("order_items.csv", header=True, inferSchema=True)

# Show Data
customers_df.show(5)
orders_df.show(5)
