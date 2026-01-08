"""
Synthetic Data Generation Template using Faker

TODO: Customize this template for your specific use case.

This template uses Faker library to generate realistic synthetic data.
Adjust tables, columns, and logic as needed for your requirements.

Environment Variables:
  SCALE_FACTOR: Float multiplier for row counts (default: 1.0)
  OUTPUT_PATH: Output directory path (default: ./data)
"""
import os
from pathlib import Path
from faker import Faker
import pandas as pd

# Initialize Faker
fake = Faker()
Faker.seed(42)  # For reproducibility

# Configuration
SCALE_FACTOR = float(os.environ.get('SCALE_FACTOR', 1.0))
OUTPUT_PATH = Path(os.environ.get('OUTPUT_PATH', './data'))
BASE_ROWS = 1000  # Base number of rows before scaling

# Calculate actual row count
NUM_ROWS = int(BASE_ROWS * SCALE_FACTOR)

print(f"Generating {NUM_ROWS} rows (scale_factor={SCALE_FACTOR})")
print(f"Output path: {OUTPUT_PATH}")


# TODO: Define your data generation functions
def generate_customers(n):
    """Generate customer data"""
    return pd.DataFrame({
        'customer_id': range(1, n + 1),
        'name': [fake.name() for _ in range(n)],
        'email': [fake.email() for _ in range(n)],
        'phone': [fake.phone_number() for _ in range(n)],
        'address': [fake.address() for _ in range(n)],
        'city': [fake.city() for _ in range(n)],
        'state': [fake.state_abbr() for _ in range(n)],
        'zip_code': [fake.zipcode() for _ in range(n)],
        'created_at': [fake.date_time_this_year() for _ in range(n)]
    })


def generate_orders(n, customer_ids):
    """Generate order data"""
    return pd.DataFrame({
        'order_id': range(1, n + 1),
        'customer_id': [fake.random_element(customer_ids) for _ in range(n)],
        'order_date': [fake.date_time_this_year() for _ in range(n)],
        'amount': [round(fake.random.uniform(10, 1000), 2) for _ in range(n)],
        'status': [fake.random_element(['pending', 'completed', 'cancelled']) for _ in range(n)]
    })


# TODO: Add more table generation functions as needed


def main():
    """Main data generation workflow"""
    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    # Generate customers
    print("Generating customers...")
    customers = generate_customers(NUM_ROWS)
    customer_output = OUTPUT_PATH / "customers"
    customer_output.mkdir(exist_ok=True)
    customers.to_parquet(customer_output / "customers.parquet", index=False)
    print(f"  ✓ Generated {len(customers)} customers")

    # Generate orders (more orders than customers)
    print("Generating orders...")
    orders = generate_orders(NUM_ROWS * 3, customers['customer_id'].tolist())
    order_output = OUTPUT_PATH / "orders"
    order_output.mkdir(exist_ok=True)
    orders.to_parquet(order_output / "orders.parquet", index=False)
    print(f"  ✓ Generated {len(orders)} orders")

    # TODO: Generate additional tables

    print(f"\n✓ Data generation complete!")
    print(f"  Output location: {OUTPUT_PATH.absolute()}")


if __name__ == "__main__":
    main()
