import pandas as pd
import random
from datetime import datetime, timedelta

# Generate Customers Data
customers = pd.DataFrame({
    "customer_id": range(1, 11),
    "name": [f"Customer_{i}" for i in range(1, 11)],
    "location": random.choices(["New York", "Los Angeles", "San Francisco", "Chicago"], k=10),
    "signup_date": [datetime(2023, random.randint(1, 12), random.randint(1, 28)).strftime("%Y-%m-%d") for _ in range(10)]
})
customers.to_csv("customers.csv", index=False)

# Generate Products Data
products = pd.DataFrame({
    "product_id": range(1, 6),
    "name": [f"Product_{i}" for i in range(1, 6)],
    "category": random.choices(["Electronics", "Clothing", "Books"], k=5),
    "price": [round(random.uniform(10, 500), 2) for _ in range(5)]
})
products.to_csv("products.csv", index=False)

# Generate Orders Data
orders = pd.DataFrame({
    "order_id": range(1, 16),
    "customer_id": random.choices(range(1, 11), k=15),
    "order_date": [(datetime(2024, random.randint(1, 3), random.randint(1, 28))).strftime("%Y-%m-%d") for _ in range(15)]
})
orders.to_csv("orders.csv", index=False)

# Generate Order Items Data
order_items = pd.DataFrame({
    "order_id": random.choices(range(1, 16), k=20),
    "product_id": random.choices(range(1, 6), k=20),
    "quantity": [random.randint(1, 5) for _ in range(20)],
    "unit_price": [round(random.uniform(10, 500), 2) for _ in range(20)]
})
order_items["total_price"] = order_items["quantity"] * order_items["unit_price"]
order_items.to_csv("order_items.csv", index=False)

print("✅ Sample Data Generated Successfully!")
