"""Debug script to examine fetched data structure."""
from data.fetcher import DataFetcher
import pandas as pd
pd.set_option('display.max_columns', None)

def debug_fetch():
    """Fetch data and examine its structure."""
    try:
        # Initialize fetcher
        fetcher = DataFetcher()
        
        # Fetch recent data
        print("Fetching data...")
        df = fetcher.fetch_recent_data()
        
        # Print info about the dataframe
        # Print raw data before normalization
        print("\nRaw data from API:")
        raw_df = fetcher.fetch_recent_data()
        print("\nRaw DataFrame Info:")
        print(raw_df.info())
        print("\nRaw column names:")
        print(raw_df.columns.tolist())
        
        # Required columns from storage.py
        required_columns = [
            'dig_ticket_number', 'permit_number', 'request_date', 'dig_date', 
            'expiration_date', 'is_emergency', 'street_name', 'street_direction',
            'street_number_from', 'street_suffix', 'dig_location',
            'latitude', 'longitude'
        ]
        
        print("\nChecking for required columns:")
        for col in required_columns:
            present = col in raw_df.columns
            print(f"{col}: {'✓' if present else '✗'}")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    debug_fetch()
