# run_etl.py

from src.extract import generate_fake_deliveries
from src.transform import transform_deliveries

def main():
    print("Starting ETL pipeline...")
    
    # Extract
    raw_data = generate_fake_deliveries(100)  # You can adjust batch size
    
    # Transform
    clean_data = transform_deliveries(raw_data)
    
    print("ETL pipeline completed. Sample data:")
    print(clean_data.head())

if __name__ == "__main__":
    main()
