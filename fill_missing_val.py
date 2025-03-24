from pyspark.sql.functions import col

# Fill missing values in categorical columns
customers_df = customers_df.fillna({"location": "Unknown"})

# Fill missing values in numerical columns
products_df = products_df.fillna({"price": 0})
order_items_df = order_items_df.fillna({"quantity": 1, "unit_price": 0})

# Drop rows where critical values are missing
orders_df = orders_df.na.drop(subset=["customer_id"])
