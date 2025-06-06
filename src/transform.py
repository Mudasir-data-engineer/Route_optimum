# src/transform.py

import pandas as pd

def normalize_address(addr: str) -> str:
    # Simple normalization: strip spaces, title case
    if not isinstance(addr, str):
        return ""
    return " ".join(addr.strip().title().split())

def transform_deliveries(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalize address
    df['address'] = df['address'].apply(normalize_address)

    # Validate weight (set to NaN if invalid)
    df.loc[df['weight_kg'] <= 0, 'weight_kg'] = pd.NA

    # Fill missing status with 'pending'
    df['status'] = df['status'].fillna('pending')

    # Convert dates to datetime
    df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce')
    df['estimated_delivery_date'] = pd.to_datetime(df['estimated_delivery_date'], errors='coerce')


    # Derived column: delivery urgency (example)
    # If estimated_delivery_date - delivery_date <= 2 days -> High, else Low
    df['delivery_urgency'] = (
        (df['estimated_delivery_date'] - df['delivery_date']).dt.days <= 2
    ).map({True: 'High', False: 'Low'})

    return df

if __name__ == '__main__':
    # Quick test run
    from extract import generate_fake_deliveries

    raw_df = generate_fake_deliveries(5)
    print("Raw Data:\n", raw_df)

    clean_df = transform_deliveries(raw_df)
    print("\nTransformed Data:\n", clean_df)
