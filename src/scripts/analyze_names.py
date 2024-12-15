"""Script to analyze variations in contractor names."""
import duckdb
from pathlib import Path
from config import config
from utils.logging import get_logger

logger = get_logger(__name__)

def analyze_names():
    """Analyze variations in contractor names."""
    try:
        db = duckdb.connect(config.db_file)
        
        # Query to get unique last names and their frequencies
        query = """
        SELECT 
            contact_last_name as name,
            COUNT(*) as frequency
        FROM read_parquet('data/*.parquet')
        WHERE contact_last_name != ''
        AND contact_last_name IS NOT NULL
        GROUP BY contact_last_name
        ORDER BY frequency DESC
        LIMIT 100
        """
        
        print("\nTop 100 Contractor Names and Frequencies:")
        print("----------------------------------------")
        result = db.execute(query).fetchall()
        for name, freq in result:
            print(f"{name}: {freq}")
            
        # Query to find similar names (case variations)
        query_case = """
        WITH name_cases AS (
            SELECT 
                contact_last_name as original_name,
                UPPER(contact_last_name) as upper_name,
                COUNT(*) as frequency
            FROM read_parquet('data/*.parquet')
            WHERE contact_last_name != ''
            AND contact_last_name IS NOT NULL
            GROUP BY contact_last_name
        )
        SELECT 
            LISTAGG(original_name, ' | ') as variations,
            SUM(frequency) as total_frequency,
            upper_name
        FROM name_cases
        GROUP BY upper_name
        HAVING COUNT(*) > 1
        ORDER BY total_frequency DESC
        LIMIT 20
        """
        
        print("\nName Variations (Case Sensitivity):")
        print("----------------------------------")
        result = db.execute(query_case).fetchall()
        for variations, freq, upper in result:
            print(f"\nVariations: {variations}")
            print(f"Total Frequency: {freq}")
            
        # Query to find names with special characters
        query_special = """
        SELECT 
            contact_last_name as name,
            COUNT(*) as frequency
        FROM read_parquet('data/*.parquet')
        WHERE contact_last_name != ''
        AND contact_last_name IS NOT NULL
        AND (
            contact_last_name LIKE '%*%'
            OR contact_last_name LIKE '%(%'
            OR contact_last_name LIKE '%)%'
            OR contact_last_name LIKE '%-%'
            OR contact_last_name LIKE '%&%'
            OR contact_last_name LIKE '%/%'
        )
        GROUP BY contact_last_name
        ORDER BY frequency DESC
        LIMIT 20
        """
        
        print("\nNames with Special Characters:")
        print("-----------------------------")
        result = db.execute(query_special).fetchall()
        for name, freq in result:
            print(f"{name}: {freq}")
            
    except Exception as e:
        logger.error(f"Error analyzing names: {str(e)}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    analyze_names()
