"""
Synthetic Data Generation: Customers and Orders
Generates realistic customer and order data with proper distributions.
No external dependencies - uses only standard library + PySpark.
"""

import numpy as np
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Reproducibility
np.random.seed(42)
random.seed(42)

# Get Spark session
spark = SparkSession.builder.getOrCreate()

N_CUSTOMERS = 1000
N_ORDERS = 10000

print(f"Generating synthetic data...")
print(f"  Customers: {N_CUSTOMERS:,}")
print(f"  Orders: {N_ORDERS:,}")
print("-" * 50)

# ============================================================
# Helper Functions
# ============================================================
FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
               'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
               'Thomas', 'Sarah', 'Charles', 'Karen', 'Emma', 'Oliver', 'Ava', 'Liam', 'Sophia',
               'Noah', 'Isabella', 'Ethan', 'Mia', 'Lucas', 'Charlotte', 'Mason', 'Amelia', 'Logan']

LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
              'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
              'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
              'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker']

DOMAINS = ['gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'icloud.com', 'proton.me']

SEGMENTS = ['Individual', 'Small Business', 'Enterprise']
SEGMENT_WEIGHTS = [0.70, 0.25, 0.05]

REGIONS = ['North America', 'Europe', 'Asia Pacific', 'Latin America']
REGION_WEIGHTS = [0.45, 0.30, 0.18, 0.07]

STATUSES = ['active', 'inactive', 'churned']
STATUS_WEIGHTS = [0.80, 0.15, 0.05]

ORDER_STATUSES = ['completed', 'shipped', 'processing', 'pending', 'cancelled', 'refunded']
ORDER_STATUS_WEIGHTS = [0.65, 0.15, 0.08, 0.05, 0.04, 0.03]

PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'crypto']
PAYMENT_WEIGHTS = [0.50, 0.25, 0.15, 0.08, 0.02]

def weighted_choice(items, weights):
    """Return a native Python string from weighted random choice."""
    return str(random.choices(items, weights=weights, k=1)[0])

def random_name():
    return random.choice(FIRST_NAMES), random.choice(LAST_NAMES)

def random_email(first, last):
    domain = random.choice(DOMAINS)
    num = random.randint(1, 999)
    return f"{first.lower()}.{last.lower()}{num}@{domain}"

def random_phone():
    return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

def random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

# ============================================================
# 1. Generate Customers
# ============================================================
print("Generating customers...")

customers = []
for i in range(N_CUSTOMERS):
    first_name, last_name = random_name()

    segment = weighted_choice(SEGMENTS, SEGMENT_WEIGHTS)
    region = weighted_choice(REGIONS, REGION_WEIGHTS)
    status = weighted_choice(STATUSES, STATUS_WEIGHTS)

    # Lifetime value correlates with segment (log-normal distribution)
    if segment == 'Enterprise':
        lifetime_value = float(np.random.lognormal(mean=10, sigma=0.6))
    elif segment == 'Small Business':
        lifetime_value = float(np.random.lognormal(mean=7, sigma=0.7))
    else:
        lifetime_value = float(np.random.lognormal(mean=5, sigma=0.8))

    # Age with realistic distribution (18-80, centered around 38)
    age = int(max(18, min(80, np.random.normal(loc=38, scale=14))))

    created_date = random_date(datetime(2022, 1, 1), datetime(2024, 11, 1))

    customers.append({
        "customer_id": f"CUST-{i+1:05d}",
        "first_name": first_name,
        "last_name": last_name,
        "email": random_email(first_name, last_name),
        "phone": random_phone(),
        "age": age,
        "segment": segment,
        "region": region,
        "status": status,
        "lifetime_value": round(lifetime_value, 2),
        "created_at": created_date.strftime("%Y-%m-%d")
    })

customers_df = spark.createDataFrame(customers)
print(f"  Created {customers_df.count():,} customers")

# Build lookup for foreign key generation
customer_lookup = {c['customer_id']: c for c in customers}
customer_ids = list(customer_lookup.keys())

# Weight by segment (Enterprise customers order more frequently)
customer_weights = []
for cid in customer_ids:
    segment = customer_lookup[cid]['segment']
    if segment == 'Enterprise':
        customer_weights.append(8.0)
    elif segment == 'Small Business':
        customer_weights.append(3.0)
    else:
        customer_weights.append(1.0)
total_weight = sum(customer_weights)
customer_weights = [w / total_weight for w in customer_weights]

# ============================================================
# 2. Generate Orders
# ============================================================
print("Generating orders...")

orders = []
for i in range(N_ORDERS):
    # Select customer (weighted by segment)
    customer_id = random.choices(customer_ids, weights=customer_weights, k=1)[0]
    customer = customer_lookup[customer_id]
    segment = customer['segment']

    # Order amount correlates with segment (log-normal)
    if segment == 'Enterprise':
        amount = float(np.random.lognormal(mean=7, sigma=0.7))
    elif segment == 'Small Business':
        amount = float(np.random.lognormal(mean=5.5, sigma=0.6))
    else:
        amount = float(np.random.lognormal(mean=4, sigma=0.8))

    # Number of items correlates with amount
    num_items = max(1, int(np.random.poisson(lam=max(1, amount/50))))

    status = weighted_choice(ORDER_STATUSES, ORDER_STATUS_WEIGHTS)
    payment_method = weighted_choice(PAYMENT_METHODS, PAYMENT_WEIGHTS)

    # Order date - more orders recently (exponential decay backwards)
    days_ago = int(min(730, np.random.exponential(scale=120)))
    order_date = datetime.now() - timedelta(days=days_ago)

    # Shipping cost correlates with amount
    shipping = round(amount * random.uniform(0.05, 0.15), 2) if status != 'cancelled' else 0.0

    # Discount - occasional discounts
    if random.random() < 0.25:
        discount = round(amount * random.uniform(0.05, 0.20), 2)
    else:
        discount = 0.0

    orders.append({
        "order_id": f"ORD-{i+1:06d}",
        "customer_id": customer_id,
        "order_date": order_date.strftime("%Y-%m-%d"),
        "status": status,
        "num_items": num_items,
        "subtotal": round(amount, 2),
        "discount": discount,
        "shipping": shipping,
        "total": round(amount - discount + shipping, 2),
        "payment_method": payment_method,
        "region": customer['region']
    })

orders_df = spark.createDataFrame(orders)
print(f"  Created {orders_df.count():,} orders")

# ============================================================
# 3. Display Sample Data
# ============================================================
print("\n" + "=" * 50)
print("SAMPLE DATA")
print("=" * 50)

print("\nCustomers (first 5):")
customers_df.show(5, truncate=False)

print("\nOrders (first 5):")
orders_df.show(5, truncate=False)

# ============================================================
# 4. Summary Statistics
# ============================================================
print("\n" + "=" * 50)
print("SUMMARY STATISTICS")
print("=" * 50)

print("\nCustomer Segments:")
customers_df.groupBy("segment").count().orderBy("count", ascending=False).show()

print("\nCustomer Regions:")
customers_df.groupBy("region").count().orderBy("count", ascending=False).show()

print("\nOrder Status:")
orders_df.groupBy("status").count().orderBy("count", ascending=False).show()

print("\nOrder Value Stats by Segment:")
orders_df.join(customers_df.select("customer_id", "segment"), "customer_id") \
    .groupBy("segment") \
    .agg(
        {"total": "avg", "order_id": "count"}
    ).show()

# ============================================================
# 5. Create Temporary Views (for querying)
# ============================================================
customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")

print("\n" + "=" * 50)
print("DATA GENERATION COMPLETE")
print("=" * 50)
print("\nTemporary views created: 'customers', 'orders'")
print("You can now query them with spark.sql()")
