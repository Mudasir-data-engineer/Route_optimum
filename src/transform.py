# tranfrom.py file:

import pandas as pd

def normalize_address(addr: str) -> str:
    # Simple normalization: strip spaces, title case
    if not isinstance(addr, str):
        return ""
    return " ".join(addr.strip().title().split())

def transform_deliveries(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalize address: column is 'delivery_address' as per extract.py
    if 'delivery_address' not in df.columns:
        raise ValueError(f"'delivery_address' column is missing. Columns found: {df.columns.tolist()}")
    df['delivery_address'] = df['delivery_address'].apply(normalize_address)

    # Validate package_weight (set to NaN if invalid)
    if 'package_weight' in df.columns:
        df.loc[df['package_weight'] <= 0, 'package_weight'] = pd.NA

    # Fill missing status with 'pending'
    if 'status' in df.columns:
        df['status'] = df['status'].fillna('pending')

    # Convert scheduled_date to datetime
    if 'scheduled_date' in df.columns:
        df['scheduled_date'] = pd.to_datetime(df['scheduled_date'], errors='coerce')

    # Add derived column: example - delivery urgency based on scheduled_date and created_at
    if 'scheduled_date' in df.columns and 'created_at' in df.columns:
        df['delivery_urgency'] = (
            (df['scheduled_date'] - pd.to_datetime(df['created_at'])).dt.days <= 2
        ).map({True: 'High', False: 'Low'})
    else:
        df['delivery_urgency'] = 'Unknown'

    return df

if __name__ == '__main__':
    from extract import extract_deliveries

    raw_df = extract_deliveries(5)
    print("Raw Data:\n", raw_df)

    clean_df = transform_deliveries(raw_df)
    print("\nTransformed Data:\n", clean_df)
