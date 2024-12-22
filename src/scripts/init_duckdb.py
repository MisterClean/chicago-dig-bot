"""Initialize DuckDB with required extensions."""
import duckdb
from config import config

def init_duckdb():
    """Initialize DuckDB with SQLite extension."""
    try:
        # First create a memory connection to install extension
        mem_conn = duckdb.connect(':memory:')
        mem_conn.execute("INSTALL sqlite;")
        mem_conn.execute("LOAD sqlite;")
        mem_conn.close()
        
        # Now connect to the file with extension pre-installed
        conn = duckdb.connect(config.db_file)
        
        print("Successfully initialized DuckDB with SQLite extension")
        conn.close()
        
    except Exception as e:
        print(f"Error initializing DuckDB: {str(e)}")
        raise

if __name__ == "__main__":
    init_duckdb()
