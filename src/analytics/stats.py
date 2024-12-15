"""Module for generating statistics and analytics from Chicago 811 dig ticket data."""
import pandas as pd
import duckdb
from typing import Dict, List, Optional
from pathlib import Path
from config import config
from utils.logging import get_logger
import re
from datetime import datetime, timedelta

logger = get_logger(__name__)

class StatsGenerationError(Exception):
    """Custom exception for statistics generation errors."""
    pass

class StatsGenerator:
    """Handles generation of statistics and analytics from dig ticket data."""
    
    def __init__(self):
        """Initialize the stats generator with database connection."""
        try:
            logger.info("Initializing StatsGenerator")
            self.db = duckdb.connect(config.db_file)
            logger.debug(f"Connected to database: {config.db_file}")
            
            # Validate analytics configuration
            self._validate_config()
            
        except Exception as e:
            error_msg = f"Failed to initialize StatsGenerator: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)
            
    def _validate_config(self) -> None:
        """Validate analytics configuration settings.
        
        Raises:
            StatsGenerationError: If configuration validation fails.
        """
        required_configs = {
            'aggregation_period': config._get_nested('analytics', 'stats', 'aggregation_period'),
            'rolling_window_days': config._get_nested('analytics', 'stats', 'rolling_window_days')
        }
        
        missing = [k for k, v in required_configs.items() if v is None]
        if missing:
            error_msg = f"Missing required analytics configuration: {missing}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)
            
        logger.debug("Analytics configuration validated successfully")
        
    def _validate_parquet_files(self) -> None:
        """Validate existence of Parquet files.
        
        Raises:
            StatsGenerationError: If no Parquet files are found.
        """
        parquet_files = list(Path(config.data_dir).glob('*.parquet'))
        if not parquet_files:
            error_msg = f"No Parquet files found in {config.data_dir}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)
            
        logger.debug(f"Found {len(parquet_files)} Parquet files")
        
    def _execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a DuckDB query safely.
        
        Args:
            query: SQL query to execute.
            params: Optional parameters for the query.
            
        Returns:
            DataFrame containing query results.
            
        Raises:
            StatsGenerationError: If query execution fails.
        """
        try:
            logger.debug(f"Executing query: {query[:200]}...")
            result = self.db.execute(query, params).df() if params else self.db.execute(query).df()
            logger.debug(f"Query returned {len(result)} rows")
            return result
            
        except Exception as e:
            error_msg = f"Query execution failed: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)

    def get_newcomer(self) -> Optional[str]:
        """Identify a contractor that appears for the first time in today's data.
        
        Returns:
            Name of the newcomer contractor if found, None otherwise.
            
        Raises:
            StatsGenerationError: If newcomer detection fails.
        """
        try:
            logger.info("Checking for newcomer contractors")
            
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            query = f"""
            WITH today_contractors AS (
                SELECT DISTINCT contact_last_name as name
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
                WHERE request_date::DATE = '{yesterday}'
            ),
            historical_contractors AS (
                SELECT DISTINCT contact_last_name as name
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
                WHERE request_date::DATE < '{yesterday}'
            ),
            newcomers AS (
                SELECT t.name, COUNT(*) as ticket_count
                FROM today_contractors t
                LEFT JOIN historical_contractors h ON t.name = h.name
                WHERE h.name IS NULL
                AND t.name != ''  -- Exclude empty names
                AND t.name IS NOT NULL  -- Exclude nulls
                GROUP BY t.name
                ORDER BY ticket_count DESC
                LIMIT 1
            )
            SELECT name, ticket_count
            FROM newcomers
            """
            
            result = self._execute_query(query)
            
            if result.empty:
                logger.info("No newcomers found")
                return None
                
            newcomer = result.iloc[0]['name']
            ticket_count = result.iloc[0]['ticket_count']
            
            # Clean up the name
            newcomer = re.sub(r'\*+$', '', newcomer)  # Remove trailing asterisks
            newcomer = newcomer.replace('*', '')  # Remove any remaining asterisks
            newcomer = ' '.join(word.capitalize() for word in newcomer.split())  # Capitalize each word
            
            logger.info(f"Found newcomer: {newcomer} with {ticket_count} tickets")
            return newcomer
            
        except Exception as e:
            error_msg = f"Failed to check for newcomers: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)

    def generate_daily_stats(self) -> pd.DataFrame:
        """Generate daily statistics from stored data.
        
        Returns:
            DataFrame containing daily statistics.
            
        Raises:
            StatsGenerationError: If statistics generation fails.
        """
        try:
            logger.info("Generating daily statistics")
            self._validate_parquet_files()
            
            agg_period = config._get_nested('analytics', 'stats', 'aggregation_period')
            window_days = config._get_nested('analytics', 'stats', 'rolling_window_days')
            
            query = f"""
            WITH daily_stats AS (
                SELECT 
                    date_trunc('{agg_period}', request_date) as date,
                    COUNT(*) as total_tickets,
                    SUM(CASE WHEN is_emergency::BOOLEAN THEN 1 ELSE 0 END) as emergency_tickets,
                    COUNT(DISTINCT dig_location) as unique_placements
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
                WHERE request_date >= CURRENT_DATE - INTERVAL '{config.soda_days_to_fetch} days'
                GROUP BY 1
                ORDER BY 1
            ),
            stats_with_basic AS (
                SELECT *,
                       ROUND(total_tickets - emergency_tickets) as regular_tickets,
                       ROUND((emergency_tickets::FLOAT / total_tickets * 100)::DECIMAL(5,2)) as emergency_percent
                FROM daily_stats
            )
            SELECT *,
                   ROUND(AVG(total_tickets) OVER 
                       (ORDER BY date ROWS BETWEEN {window_days-1} PRECEDING AND CURRENT ROW)) 
                       as rolling_avg_total,
                   ROUND(AVG(emergency_tickets) OVER 
                       (ORDER BY date ROWS BETWEEN {window_days-1} PRECEDING AND CURRENT ROW)) 
                       as rolling_avg_emergency
            FROM stats_with_basic
            """
            
            result = self._execute_query(query)
            
            if result.empty:
                logger.warning("No data found for the specified time period")
            else:
                logger.info(f"Generated daily stats for {len(result)} days")
                logger.debug(f"Latest stats: {result.iloc[-1].to_dict()}")
                
            return result
            
        except Exception as e:
            error_msg = f"Failed to generate daily statistics: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)

    def get_day_of_week_comparison(self, date: str) -> Dict:
        """Get comparison of actual vs historical average for a specific date.
        
        Args:
            date: Date to analyze in YYYY-MM-DD format
            
        Returns:
            Dictionary containing comparison metrics
            
        Raises:
            StatsGenerationError: If comparison fails
        """
        try:
            logger.info(f"Generating day of week comparison for {date}")
            
            query = f"""
            WITH daily_counts AS (
                SELECT 
                    request_date::DATE as date,
                    DAYNAME(request_date) as day_name,
                    COUNT(*) as total_tickets,
                    SUM(CASE WHEN is_emergency::BOOLEAN THEN 1 ELSE 0 END) as emergency_tickets,
                    COUNT(*) - SUM(CASE WHEN is_emergency::BOOLEAN THEN 1 ELSE 0 END) as regular_tickets
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
                GROUP BY 1, 2
            ),
            historical_averages AS (
                SELECT
                    day_name,
                    ROUND(AVG(total_tickets)) as avg_total,
                    ROUND(AVG(emergency_tickets)) as avg_emergency,
                    ROUND(AVG(regular_tickets)) as avg_regular
                FROM daily_counts
                WHERE date < '{date}'::DATE
                GROUP BY 1
            ),
            actual_values AS (
                SELECT *
                FROM daily_counts
                WHERE date = '{date}'::DATE
            )
            SELECT 
                a.day_name,
                ROUND(a.total_tickets) as actual_total,
                ROUND(a.emergency_tickets) as actual_emergency,
                ROUND(a.regular_tickets) as actual_regular,
                h.avg_total as expected_total,
                h.avg_emergency as expected_emergency,
                h.avg_regular as expected_regular,
                ROUND(((a.total_tickets - h.avg_total) / h.avg_total * 100)::DECIMAL(5,2)) as total_diff_percent,
                ROUND(((a.emergency_tickets - h.avg_emergency) / h.avg_emergency * 100)::DECIMAL(5,2)) as emergency_diff_percent,
                ROUND(((a.regular_tickets - h.avg_regular) / h.avg_regular * 100)::DECIMAL(5,2)) as regular_diff_percent
            FROM actual_values a
            JOIN historical_averages h ON a.day_name = h.day_name
            """
            
            result = self._execute_query(query)
            
            if result.empty:
                logger.warning(f"No data found for date {date}")
                return {}
                
            comparison = result.iloc[0].to_dict()
            logger.info(f"Generated comparison for {date}: {comparison}")
            
            return comparison
            
        except Exception as e:
            error_msg = f"Failed to generate day of week comparison: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)
            
    def get_contractor_leaderboard(self, limit: int = 10) -> Dict[str, List[Dict]]:
        """Generate leaderboards for contractors.
        
        Args:
            limit: Maximum number of entries per leaderboard.
            
        Returns:
            Dictionary containing contractor leaderboards.
            
        Raises:
            StatsGenerationError: If leaderboard generation fails.
        """
        try:
            logger.info(f"Generating contractor leaderboard (limit: {limit})")
            self._validate_parquet_files()
            
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            query = f"""
            WITH contractor_stats AS (
                SELECT 
                    contact_last_name as name,
                    COUNT(*) as total_tickets,
                    SUM(CASE WHEN is_emergency::BOOLEAN THEN 1 ELSE 0 END) as emergency_tickets,
                    COUNT(DISTINCT street_name) as unique_streets
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
                WHERE request_date::DATE = '{yesterday}'
                GROUP BY 1
            )
            SELECT
                (SELECT ARRAY_AGG(ROW(name, total_tickets)::VARCHAR)
                 FROM (SELECT * FROM contractor_stats 
                       ORDER BY total_tickets DESC LIMIT {limit})) as top_overall,
                (SELECT ARRAY_AGG(ROW(name, emergency_tickets)::VARCHAR)
                 FROM (SELECT * FROM contractor_stats 
                       ORDER BY emergency_tickets DESC LIMIT {limit})) as top_emergency,
                (SELECT ARRAY_AGG(ROW(name, unique_streets)::VARCHAR)
                 FROM (SELECT * FROM contractor_stats 
                       ORDER BY unique_streets DESC LIMIT {limit})) as top_streets
            """
            
            result = self.db.execute(query).fetchone()
            leaderboards = {
                'overall': [self._parse_record(r) for r in result[0]],
                'emergency': [self._parse_record(r) for r in result[1]],
                'streets': [self._parse_record(r) for r in result[2]]
            }
            
            logger.info("Contractor leaderboard generated successfully")
            logger.debug(f"Top contractor: {leaderboards['overall'][0] if leaderboards['overall'] else None}")
            
            return leaderboards
            
        except Exception as e:
            error_msg = f"Failed to generate contractor leaderboard: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)
            
    def _parse_record(self, record_str: str) -> Dict:
        """Parse a (key, value) record string from DuckDB into a dict.
        
        Args:
            record_str: String containing the record in (key,value) format.
            
        Returns:
            Dictionary containing parsed record.
        """
        if not record_str:
            return {}
            
        try:
            # Remove outer parentheses
            record_str = record_str.strip('()')
            
            # Find the last number in the string
            match = re.search(r'^(.*?)[\s,]+(\d+)$', record_str)
            if not match:
                logger.warning(f"Failed to parse record '{record_str}': no match found")
                return {}
                
            name = match.group(1).strip()
            count = int(match.group(2))
            
            # Clean up the name
            name = re.sub(r'\*+$', '', name)  # Remove trailing asterisks
            name = name.replace('*', '')  # Remove any remaining asterisks
            name = ' '.join(word.capitalize() for word in name.split())  # Capitalize each word
            
            return {
                'name': name,
                'count': count
            }
        except Exception as e:
            logger.warning(f"Failed to parse record '{record_str}': {str(e)}")
            return {}
            
    def __del__(self):
        """Cleanup database connection on object destruction."""
        try:
            if hasattr(self, 'db'):
                self.db.close()
                logger.debug("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")
