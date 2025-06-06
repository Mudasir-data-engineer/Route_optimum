# src/load.py

import pandas as pd
from sqlalchemy import create_engine, text
import os

def get_db_engine():
    # Update these values based on your docker-compose or pgAdmin settings
    DB_USER = os.getenv("POSTGRES_USER", "routeuser")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")
    DB_HOST = os.getenv("POSTGRES_HOST", "route-optimum-db-1")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME = os.getenv("POSTGRES_DB", "routeoptimum")

    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def load_deliveries_to_db(df: pd.DataFrame):
    engine = get_db_engine()

    with engine.connect() as conn:
        # Create table if it doesn't exist
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS deliveries (
            delivery_id UUID PRIMARY KEY,
            customer_id UUID,
            address TEXT,
            delivery_date DATE,
            estimated_delivery_date DATE,
            package_weight FLOAT,
            status TEXT,
            delivery_urgency TEXT
        )
        """))

    # Insert data
    df.to_sql("deliveries", engine, if_exists="append", index=False)
    print("✅ Data loaded into PostgreSQL")

