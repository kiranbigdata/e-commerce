# Remove duplicate rows
customers_df = customers_df.dropDuplicates()
orders_df = orders_df.dropDuplicates()
products_df = products_df.dropDuplicates()
order_items_df = order_items_df.dropDuplicates()
