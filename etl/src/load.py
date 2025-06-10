import pandas as pd
from sqlalchemy import create_engine, text
import os

def get_db_engine():
    # Use environment variables with sensible defaults
    DB_USER = os.getenv("POSTGRES_USER", "routeuser")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "routepass")
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")  # ✅ changed from "db" to "localhost"
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME = os.getenv("POSTGRES_DB", "routeoptimum")

    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def load_deliveries(df: pd.DataFrame):
    print("▶️ Starting load_deliveries")

    engine = get_db_engine()

    with engine.connect() as conn:
        # Create table if it doesn't exist, matching the ETL schema
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS deliveries (
            id UUID PRIMARY KEY,
            customer_id INTEGER,
            route_id INTEGER,
            delivery_address TEXT,
            scheduled_date DATE,
            status TEXT,
            package_weight FLOAT,
            created_at TIMESTAMP,
            delivery_urgency TEXT
        )
        """))

        # Query existing route IDs from routes table
        existing_routes = pd.read_sql("SELECT id FROM routes", conn)
        print("ℹ️ Existing routes fetched:")
        print(existing_routes)

    # Normalize delivery_urgency column if it exists
    if "delivery_urgency" in df.columns:
        df["delivery_urgency"] = df["delivery_urgency"].str.lower()

    # Filter df to keep only rows with valid route_id
    df_to_load = df.copy()
    df_to_load = df_to_load[df_to_load['route_id'].isin(existing_routes['id'])]
    print(f"ℹ️ Filtered df_to_load (only valid route_ids): {len(df_to_load)} rows")
    print(df_to_load.head())

    # Insert data into the deliveries table
    df_to_load.to_sql("deliveries", engine, if_exists="append", index=False)
    print("✅ Data loaded into PostgreSQL")

    print("▶️ Finished load_deliveries")

if __name__ == "__main__":
    # For testing purposes, create a sample DataFrame similar to your expected data
    sample_data = {
        "id": ["123e4567-e89b-12d3-a456-426614174000"],  # Use a valid UUID string here
        "customer_id": [1],
        "route_id": [1],  # Make sure this route_id exists in your routes table
        "delivery_address": ["123 Main St"],
        "scheduled_date": ["2025-06-10"],
        "status": ["pending"],
        "package_weight": [2.5],
        "created_at": ["2025-06-05 10:00:00"],
        "delivery_urgency": ["high"]
    }
    df = pd.DataFrame(sample_data)

    # Convert date strings to datetime objects
    df["scheduled_date"] = pd.to_datetime(df["scheduled_date"])
    df["created_at"] = pd.to_datetime(df["created_at"])

    load_deliveries(df)
