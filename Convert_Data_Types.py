from pyspark.sql.types import IntegerType, DoubleType, DateType

# Convert columns to correct types
customers_df = customers_df.withColumn("customer_id", col("customer_id").cast(IntegerType()))
products_df = products_df.withColumn("price", col("price").cast(DoubleType()))
orders_df = orders_df.withColumn("order_date", col("order_date").cast(DateType()))
order_items_df = order_items_df.withColumn("quantity", col("quantity").cast(IntegerType()))
order_items_df = order_items_df.withColumn("unit_price", col("unit_price").cast(DoubleType()))
order_items_df = order_items_df.withColumn("total_price", col("total_price").cast(DoubleType()))
