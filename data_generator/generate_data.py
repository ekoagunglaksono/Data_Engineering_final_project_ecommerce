import os
import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# Direktori untuk menyimpan file CSV
DATA_DIR = "/opt/airflow/data"
os.makedirs(DATA_DIR, exist_ok=True)

fake = Faker()

# Jumlah data
NUM_USERS = 10000
NUM_ORDERS = 50000

def run_generator():
    print("Mulai generate data ecommerce...")

    # Suppliers
    suppliers = [{"supplier_id": i+1, "name": fake.company()} for i in range(20)]
    pd.DataFrame(suppliers).to_csv(f"{DATA_DIR}/suppliers.csv", index=False)
    print(f"Saved suppliers.csv ({len(suppliers)} rows)")

    # Users
    users = []
    for i in range(NUM_USERS):
        users.append({
            "user_id": i+1,
            "name": fake.name(),
            "email": fake.email(),
            "address": fake.address().replace("\n", ", "),
            "created_at": fake.date_between(start_date="-2y", end_date="today")
        })
    pd.DataFrame(users).to_csv(f"{DATA_DIR}/users.csv", index=False)
    print(f"Saved users.csv ({len(users)} rows)")

    # Products
    products = []
    for i in range(50):
        products.append({
            "product_id": i+1,
            "name": fake.word().capitalize(),
            "price": round(random.uniform(10, 500), 2),
            "supplier_id": random.randint(1, 20),
            "created_at": fake.date_between(start_date="-2y", end_date="today")
        })
    pd.DataFrame(products).to_csv(f"{DATA_DIR}/products.csv", index=False)
    print(f"Saved products.csv ({len(products)} rows)")

    # Orders
    orders = []
    for i in range(NUM_ORDERS):
        orders.append({
            "order_id": i+1,
            "user_id": random.randint(1, NUM_USERS),
            "order_date": fake.date_between(start_date="-1y", end_date="today"),
            "status": random.choice(["completed", "pending", "canceled"])
        })
    pd.DataFrame(orders).to_csv(f"{DATA_DIR}/orders.csv", index=False)
    print(f"Saved orders.csv ({len(orders)} rows)")

    # Order Items
    order_items = []
    for i in range(NUM_ORDERS):
        for _ in range(random.randint(1, 3)):
            order_items.append({
                "order_item_id": len(order_items) + 1,
                "order_id": i+1,
                "product_id": random.randint(1, 50),
                "quantity": random.randint(1, 5)
            })
    pd.DataFrame(order_items).to_csv(f"{DATA_DIR}/order_items.csv", index=False)
    print(f"Saved order_items.csv ({len(order_items)} rows)")

    # Payments
    payments = []
    for i in range(int(NUM_ORDERS * 0.8)):  # 80% order bayar
        payments.append({
            "payment_id": i+1,
            "order_id": random.randint(1, NUM_ORDERS),
            "amount": round(random.uniform(10, 1000), 2),
            "payment_date": fake.date_between(start_date="-1y", end_date="today"),
            "is_success": random.choice([True, True, False])  # mostly sukses
        })
    pd.DataFrame(payments).to_csv(f"{DATA_DIR}/payments.csv", index=False)
    print(f"Saved payments.csv ({len(payments)} rows)")

    # Shipments
    shipments = []
    for i in range(int(NUM_ORDERS * 0.3)):  # 30% punya shipment
        shipments.append({
            "shipment_id": i+1,
            "order_id": random.randint(1, NUM_ORDERS),
            "shipping_status": random.choice(["in_transit", "delivered", "returned"]),
            "shipping_cost": round(random.uniform(5, 50), 2)
        })
    pd.DataFrame(shipments).to_csv(f"{DATA_DIR}/shipments.csv", index=False)
    print(f"Saved shipments.csv ({len(shipments)} rows)")

    # Reviews
    reviews = []
    for i in range(int(NUM_ORDERS * 0.25)):  # 25% order ada review
        reviews.append({
            "review_id": i+1,
            "order_item_id": random.randint(1, len(order_items)),
            "rating": random.randint(1, 5),
            "comment": fake.sentence()
        })
    pd.DataFrame(reviews).to_csv(f"{DATA_DIR}/reviews.csv", index=False)
    print(f"Saved reviews.csv ({len(reviews)} rows)")

    print("Data generation selesai! File CSV ada di:", DATA_DIR)


if __name__ == "__main__":
    run_generator()
