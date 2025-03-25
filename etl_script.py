from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.types import IntegerType, DoubleType, DateType

# Initialize Spark Session
spark = SparkSession.builder.appName("ECommerceETL").getOrCreate()

# 1️⃣ EXTRACT: Read raw data from CSV
customers_df = spark.read.csv("customers.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
products_df = spark.read.csv("products.csv", header=True, inferSchema=True)
order_items_df = spark.read.csv("order_items.csv", header=True, inferSchema=True)

# 2️⃣ TRANSFORM: Clean and process data
customers_df = customers_df.dropDuplicates().fillna({"location": "Unknown"})
products_df = products_df.dropDuplicates().fillna({"price": 0})
orders_df = orders_df.dropDuplicates().na.drop(subset=["customer_id"])
order_items_df = order_items_df.dropDuplicates().fillna({"quantity": 1, "unit_price": 0})

# Convert data types
customers_df = customers_df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
products_df = products_df.withColumn("price", col("price").cast(DoubleType()))
orders_df = orders_df.withColumn("order_date", col("order_date").cast(DateType()))
order_items_df = order_items_df.withColumn("quantity", col("quantity").cast(IntegerType()))
order_items_df = order_items_df.withColumn("unit_price", col("unit_price").cast(DoubleType()))
order_items_df = order_items_df.withColumn("total_price", col("total_price").cast(DoubleType()))

# Aggregate total revenue per order
order_revenue_df = order_items_df.groupBy("order_id").agg(sum("total_price").alias("total_order_value"))

# Join with orders to create the fact table
fact_orders_df = orders_df.join(order_revenue_df, "order_id", "left")

# 3️⃣ LOAD: Save transformed data to Parquet
customers_df.write.mode("overwrite").parquet("output/customers.parquet")
products_df.write.mode("overwrite").parquet("output/products.parquet")
orders_df.write.mode("overwrite").parquet("output/orders.parquet")
order_items_df.write.mode("overwrite").parquet("output/order_items.parquet")
fact_orders_df.write.mode("overwrite").parquet("output/fact_orders.parquet")

print("✅ ETL Pipeline Completed Successfully!")
