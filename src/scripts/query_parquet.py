import duckdb
import pandas as pd

# Connect to DuckDB
con = duckdb.connect()

# Query the parquet file
query = """
SELECT 
    dig_ticket_number as 'Ticket #',
    strftime(request_date, '%Y-%m-%d') as 'Request Date',
    strftime(dig_date, '%Y-%m-%d') as 'Dig Date',
    street_number_from || ' ' || street_direction || ' ' || street_name || 
    CASE WHEN street_suffix IS NOT NULL THEN ' ' || street_suffix ELSE '' END as 'Location',
    CASE WHEN is_emergency THEN 'Yes' ELSE 'No' END as 'Emergency',
    round(latitude, 4) as 'Lat',
    round(longitude, 4) as 'Long'
FROM read_parquet('data/chicago811_permits.parquet')
LIMIT 5
"""

# Execute query and convert to pandas for nice formatting
df = con.execute(query).df()

# Set display options for better formatting
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Print formatted results
print("\nMost Recent Chicago 811 Dig Permits:\n")
print(df.to_string(index=False))
