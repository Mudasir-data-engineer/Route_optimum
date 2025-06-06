# src/extract.py

from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

fake = Faker()

def generate_fake_deliveries(batch_size=100):
    deliveries = []
    for _ in range(batch_size):
        delivery_id = fake.uuid4()
        customer_name = fake.name()
        address = fake.address().replace('\n', ', ')
        weight_kg = round(random.uniform(0.1, 50.0), 2)  # Package weight between 0.1kg to 50kg
        delivery_date = fake.date_between(start_date='today', end_date='+30d')
        # Estimated delivery time based on random factor (e.g., 1 to 5 days)
        estimated_delivery_time_days = random.randint(1, 5)
        estimated_delivery_date = delivery_date + timedelta(days=estimated_delivery_time_days)
        status = random.choice(['pending', 'in_transit', 'delivered', 'failed'])

        deliveries.append({
            'delivery_id': delivery_id,
            'customer_name': customer_name,
            'address': address,
            'weight_kg': weight_kg,
            'delivery_date': delivery_date,
            'estimated_delivery_date': estimated_delivery_date,
            'status': status
        })

    df = pd.DataFrame(deliveries)
    return df


if __name__ == '__main__':
    df = generate_fake_deliveries(5)
    print(df)
