# extract.py file:

from faker import Faker
import pandas as pd
import random
from datetime import datetime

fake = Faker()

def extract_deliveries(batch_size=100):
    deliveries = []
    for _ in range(batch_size):
        id_ = fake.uuid4()  # corresponds to "id" in schema
        customer_id = fake.random_int(min=1, max=1000)
        route_id = fake.random_int(min=1, max=100)  # Assuming route IDs between 1 and 100
        delivery_address = fake.address().replace('\n', ', ')
        package_weight = round(random.uniform(0.1, 50.0), 2)
        scheduled_date = fake.date_between(start_date='today', end_date='+30d')
        status = random.choice(['pending', 'in_transit', 'delivered', 'failed'])
        created_at = datetime.now()

        deliveries.append({
            'id': id_,
            'customer_id': customer_id,
            'route_id': route_id,
            'delivery_address': delivery_address,
            'scheduled_date': scheduled_date,
            'status': status,
            'package_weight': package_weight,
            'created_at': created_at
        })

    df = pd.DataFrame(deliveries)
    return df


if __name__ == '__main__':
    df = extract_deliveries(5)
    print(df)
