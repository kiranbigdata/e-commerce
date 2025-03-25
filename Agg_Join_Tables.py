from pyspark.sql.functions import sum

# Aggregate total revenue per order
order_revenue_df = order_items_df.groupBy("order_id").agg(sum("total_price").alias("total_order_value"))

# Join with orders to create the fact table
fact_orders_df = orders_df.join(order_revenue_df, "order_id", "left")

# Show the final transformed data
fact_orders_df.show()
