import pandas as pd
from sqlalchemy import create_engine, text
import os
import traceback

def get_db_engine():
    DB_USER = os.getenv("POSTGRES_USER", "routeuser")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "routepass")
    DB_HOST = os.getenv("POSTGRES_HOST", "db")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME = os.getenv("POSTGRES_DB", "routeoptimum")

    connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_string)

def load_customers(df: pd.DataFrame):
    print("▶️ Starting load_customers")

    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # Create customers table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS customers (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) UNIQUE,
                    phone VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            print("📁 'customers' table ensured")

        # Insert customers (upsert can be implemented if needed)
        df.to_sql("customers", engine, if_exists="append", index=False)
        print(f"✅ Inserted {len(df)} customers")

    except Exception:
        print("❌ Exception in load_customers:")
        print(traceback.format_exc())
        raise

    print("🏁 Finished load_customers")

def load_deliveries(df: pd.DataFrame):
    print("▶️ Starting load_deliveries")

    try:
        engine = get_db_engine()
        print("🔌 Database engine created")

        with engine.connect() as conn:
            print("📡 Connected to database")

            # Create deliveries table with appropriate schema
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS deliveries (
                    id UUID PRIMARY KEY,
                    customer_id INTEGER REFERENCES customers(id),
                    route_id INTEGER,
                    delivery_address TEXT,
                    scheduled_date DATE,
                    status VARCHAR,
                    package_weight NUMERIC,
                    created_at TIMESTAMP,
                    delivery_urgency VARCHAR
                )
            """))
            print("📁 'deliveries' table ensured")

            # Fetch valid route ids to filter data
            existing_routes = pd.read_sql("SELECT id FROM routes", conn)
            print(f"✅ Existing routes fetched: {len(existing_routes)} found")

        # Normalize delivery_urgency to lowercase if exists
        if "delivery_urgency" in df.columns:
            df["delivery_urgency"] = df["delivery_urgency"].str.lower()

        # Filter deliveries with valid route_id only
        df_to_load = df[df['route_id'].isin(existing_routes['id'])]
        print(f"📦 Rows to insert (filtered by valid route_ids): {len(df_to_load)}")

        df_to_load.to_sql("deliveries", engine, if_exists="append", index=False)
        print("✅ Data successfully loaded into PostgreSQL")

    except Exception:
        print("❌ Exception occurred in load_deliveries:")
        print(traceback.format_exc())
        raise

    print("🏁 Finished load_deliveries")

if __name__ == "__main__":
    try:
        print("🧪 Generating sample data for test run...")

        # Sample customers matching the schema
        sample_customers = {
            "id": [1, 2],
            "name": ["Alice Johnson", "Bob Smith"],
            "email": ["alice@example.com", "bob@example.com"],
            "phone": ["123-456-7890", "987-654-3210"],
            "created_at": [pd.Timestamp("2025-01-01"), pd.Timestamp("2025-01-02")]
        }
        df_customers = pd.DataFrame(sample_customers)

        # Load customers first
        load_customers(df_customers)

        # Sample deliveries data
        sample_deliveries = {
            "id": ["123e4567-e89b-12d3-a456-426614174000"],
            "customer_id": [1],
            "route_id": [1],
            "delivery_address": ["123 Main St"],
            "scheduled_date": [pd.Timestamp("2025-06-10")],
            "status": ["pending"],
            "package_weight": [2.5],
            "created_at": [pd.Timestamp("2025-06-05 10:00:00")],
            "delivery_urgency": ["high"]
        }
        df_deliveries = pd.DataFrame(sample_deliveries)

        load_deliveries(df_deliveries)

    except Exception:
        print("❌ Error during __main__ test run:")
        print(traceback.format_exc())
        raise
