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
    
    # Common business suffixes to remove - removed leading \s+ to handle cases without spaces
    BUSINESS_SUFFIXES = [
        r'(?:,\s+)?INC(?:ORPORATED)?\.?',
        r'(?:,\s+)?LLC\.?',
        r'(?:,\s+)?CO(?:MPANY)?\.?',
        r'(?:,\s+)?CORP(?:ORATION)?\.?',
        r'(?:,\s+)?LTD\.?',
    ]
    
    # Known name variations to normalize
    NAME_MAPPINGS = {
        # Utilities
        "PEOPLES GAS": "Peoples Gas",
        "INTEGRYS ENERGY GROUP / PEOPLES GAS": "Peoples Gas",
        "INTEGRYS ENERGY GROUP/PEOPLE GAS": "Peoples Gas",
        "COMED NORTH": "ComEd",
        "COMED": "ComEd",
        "COM ED": "ComEd",
        "COM-ED": "ComEd",
        
        # City Departments
        "DWM": "Department of Water Management",
        "CITY OF CHICAGO DEPT OF WATER": "Department of Water Management",
        "CITY OF CHICAGO WATER DEPARTMENT": "Department of Water Management",
        "CITY OF CHICAGO (DEPT OF WATER MANAGEMENT)": "Department of Water Management",
        "CITY OF CHICAGO WATER DEPT": "Department of Water Management",
        "CDOT - IN HOUSE CONSTRUCTION": "CDOT - In-House Construction",
        "CDOT - IN-HOUSE CONSTRUCTION": "CDOT - In-House Construction",
        "CDOT - INHOUSECONSTRUCTION": "CDOT - In-House Construction",
        "CDOT-IN HOUSE CONSTRUCTION": "CDOT - In-House Construction",
        "CDOT-INHOUSECONSTRUCTION": "CDOT - In-House Construction",
        "CDOT-SIGN MANAGEMENT": "CDOT - Sign Management",
        "CDOT - SIGN MANAGEMENT": "CDOT - Sign Management",
        
        # Construction Companies
        "SEVEN-D CONSTRUCTION": "Seven-D Construction",
        "SEVEN D CONSTRUCTION": "Seven-D Construction",
        "M & J ASPHALT": "M&J Asphalt",
        "M&J ASPHALT": "M&J Asphalt",
        "G & V CONST": "G&V Construction",
        "G&V CONST": "G&V Construction",
        "RELIABLE CONTRACTING & EQUIPMENT": "Reliable Contracting & Equipment",
        "RELIABLECONTRACTING&EQUIPMENT": "Reliable Contracting & Equipment",
        "RELIABLE CONTRACTING AND EQUIPMENT": "Reliable Contracting & Equipment",
        "MILLER PIPELINE": "Miller Pipeline",
    }
    
    # Words that should always be capitalized a certain way
    WORD_CAPITALIZATIONS = {
        "OF": "of",
        "AND": "and",
        "THE": "the",
        "IN": "in",
        "AT": "at",
        "BY": "by",
        "FOR": "for",
        "WITH": "with",
        "LLC": "LLC",
        "INC": "Inc",
        "CO": "Co",
        "DEPT": "Dept",
        "DBA": "dba",
    }
    
    # Parenthetical suffixes to preserve - ordered by specificity
    PRESERVE_SUFFIXES = [
        r'\(SL-\d+\)',  # Specific SL numbers
        r'\(SL\)',      # Generic SL
        r'\(SEAL\)',
        r'\(OVERSIZE\)',
    ]
    
    # Parenthetical suffixes to remove
    REMOVE_SUFFIXES = [
        r'\(HOMEOWNER\)',
        r'\(CONSTRUCTION\)',
        r'\(COMMERCIAL\)',
        r'\(LESSEE\)',
        r'\(LOT OWNER\)',
        r'\(DWM CONTRACT\)',
    ]
    
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
        """Validate analytics configuration settings."""
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
        """Validate existence of Parquet files."""
        parquet_files = list(Path(config.data_dir).glob('*.parquet'))
        if not parquet_files:
            error_msg = f"No Parquet files found in {config.data_dir}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)
            
        logger.debug(f"Found {len(parquet_files)} Parquet files")
        
    def _execute_query(self, query: str, params: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a DuckDB query safely."""
        try:
            logger.debug(f"Executing query: {query[:200]}...")
            result = self.db.execute(query, params).df() if params else self.db.execute(query).df()
            logger.debug(f"Query returned {len(result)} rows")
            return result
            
        except Exception as e:
            error_msg = f"Query execution failed: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)

    def _normalize_name(self, name: str) -> str:
        """Normalize a contractor name to a standard format.
        
        Args:
            name: Raw contractor name.
            
        Returns:
            Normalized contractor name.
        """
        if not name:
            return name
            
        # Convert to uppercase for consistent comparison
        name = name.upper()
        
        # Remove trailing asterisks and other special characters
        name = re.sub(r'\*+$', '', name)
        name = name.replace('*', '')
        
        # Normalize whitespace and remove trailing/leading spaces
        name = re.sub(r'\s+', ' ', name)
        name = name.strip()
        
        # Extract any parenthetical suffixes to preserve
        preserved_suffix = ''
        for suffix_pattern in self.PRESERVE_SUFFIXES:
            match = re.search(suffix_pattern, name, re.IGNORECASE)
            if match:
                preserved_suffix = f" {match.group()}"
                name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE)
                break
        
        # Remove unwanted parenthetical suffixes
        for suffix_pattern in self.REMOVE_SUFFIXES:
            name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE)
        
        # Remove any remaining parenthetical expressions
        name = re.sub(r'\([^)]+\)', '', name)
        
        # Check for known variations first
        # Try exact match first
        if name in self.NAME_MAPPINGS:
            return f"{self.NAME_MAPPINGS[name]}{preserved_suffix}"
            
        # Then try normalized comparison (remove extra spaces, standardize separators)
        normalized_input = re.sub(r'[-\s]+', ' ', name)
        for pattern, replacement in self.NAME_MAPPINGS.items():
            normalized_pattern = re.sub(r'[-\s]+', ' ', pattern.upper())
            if normalized_input == normalized_pattern:
                return f"{replacement}{preserved_suffix}"
        
        # Remove common business suffixes
        for suffix_pattern in self.BUSINESS_SUFFIXES:
            name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE)
        
        # Normalize ampersands and other conjunctions
        name = name.replace(' AND ', ' & ')
        name = name.replace('&', ' & ')  # Add spaces around ampersands
        name = re.sub(r'\s+', ' ', name)  # Clean up any resulting double spaces
        
        # Split into words and apply specific capitalization rules
        words = name.split()
        normalized_words = []
        for i, word in enumerate(words):
            # Keep abbreviations in uppercase
            if len(word) <= 3 and word.isupper() and word not in self.WORD_CAPITALIZATIONS:
                normalized_words.append(word)
            # Special case for McXxx names
            elif word.upper().startswith('MC'):
                normalized_words.append('Mc' + word[2:].capitalize())
            # Apply specific capitalization rules
            elif word in self.WORD_CAPITALIZATIONS:
                # Always capitalize first word
                if i == 0:
                    normalized_words.append(word.capitalize())
                else:
                    normalized_words.append(self.WORD_CAPITALIZATIONS[word])
            # Special case for hyphenated words
            elif '-' in word:
                parts = word.split('-')
                normalized_parts = [p.capitalize() for p in parts]
                normalized_words.append('-'.join(normalized_parts))
            else:
                normalized_words.append(word.capitalize())
        
        name = ' '.join(normalized_words)
        
        # Add back any preserved suffix
        if preserved_suffix:
            name = f"{name}{preserved_suffix}"
        
        return name.strip()
            
    def _parse_record(self, record_str: str) -> Dict:
        """Parse a record string from DuckDB into a dict.
        
        Args:
            record_str: String containing the record.
            
        Returns:
            Dictionary containing parsed record.
        """
        if not record_str:
            return {}
            
        try:
            # Extract name and count using regex
            # Updated pattern to better handle special characters and asterisks
            match = re.search(r"(?:\{'': |^\()([^,]+?)(?:\*+)?,\s*(?:'': |)(\d+)(?:\}|\))", record_str)
            if not match:
                logger.warning(f"Failed to parse record '{record_str}': no match found")
                return {}
                
            name = match.group(1).strip()
            count = int(match.group(2))
            
            # Normalize the name
            name = self._normalize_name(name)
            
            return {
                'name': name,
                'count': count
            }
        except Exception as e:
            logger.warning(f"Failed to parse record '{record_str}': {str(e)}")
            return {}

    def generate_daily_stats(self) -> Dict:
        """Generate basic statistics for the current day's permits.
        
        Returns:
            Dictionary containing daily statistics including:
            - total_permits: Total number of permits
            - emergency_permits: Number of emergency permits
            - regular_permits: Number of regular permits
            - unique_contractors: Number of unique contractors
            - unique_streets: Number of unique streets
        """
        try:
            logger.info("Generating daily statistics")
            self._validate_parquet_files()
            
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            query = f"""
            SELECT
                COUNT(*) as total_permits,
                SUM(CASE WHEN is_emergency::BOOLEAN THEN 1 ELSE 0 END) as emergency_permits,
                COUNT(DISTINCT contact_last_name) as unique_contractors,
                COUNT(DISTINCT street_name) as unique_streets
            FROM read_parquet('{str(config.data_dir)}/*.parquet')
            WHERE request_date::DATE = '{yesterday}'
            """
            
            result = self._execute_query(query)
            
            if result.empty:
                raise StatsGenerationError("No data found for daily statistics")
            
            stats = {
                'total_permits': int(result['total_permits'].iloc[0]),
                'emergency_permits': int(result['emergency_permits'].iloc[0]),
                'regular_permits': int(result['total_permits'].iloc[0] - result['emergency_permits'].iloc[0]),
                'unique_contractors': int(result['unique_contractors'].iloc[0]),
                'unique_streets': int(result['unique_streets'].iloc[0])
            }
            
            logger.info("Daily statistics generated successfully")
            logger.debug(f"Daily stats: {stats}")
            
            return stats
            
        except Exception as e:
            error_msg = f"Failed to generate daily statistics: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)

    def get_day_of_week_comparison(self, date_str: str) -> Dict:
        """Compare permit counts for a given date with historical averages.
        
        Args:
            date_str: Date string in YYYY-MM-DD format to analyze
            
        Returns:
            Dictionary containing comparison data including:
            - day_name: Name of the day of week
            - actual_total: Total permits for the given date
            - actual_emergency: Emergency permits for the given date
            - actual_regular: Regular permits for the given date
            - avg_total: Average total permits for this day of week
            - avg_emergency: Average emergency permits for this day of week
            - avg_regular: Average regular permits for this day of week
            - total_diff_percent: Percentage difference in total permits vs average
            - emergency_diff_percent: Percentage difference in emergency permits vs average
            - regular_diff_percent: Percentage difference in regular permits vs average
        """
        try:
            logger.info(f"Generating day of week comparison for {date_str}")
            self._validate_parquet_files()
            
            # Parse the date
            date = datetime.strptime(date_str, '%Y-%m-%d')
            day_of_week = date.strftime('%A')
            
            # Calculate the date range for historical comparison
            today = datetime.now().strftime('%Y-%m-%d')
            rolling_window = config._get_nested('analytics', 'stats', 'rolling_window_days')
            start_date = (datetime.now() - timedelta(days=rolling_window)).strftime('%Y-%m-%d')
            
            # Get actual counts for the given date
            actual_query = f"""
            SELECT
                COUNT(*) as total_permits,
                SUM(CASE WHEN is_emergency::BOOLEAN THEN 1 ELSE 0 END) as emergency_permits,
                SUM(CASE WHEN NOT is_emergency::BOOLEAN THEN 1 ELSE 0 END) as regular_permits
            FROM read_parquet('{str(config.data_dir)}/*.parquet')
            WHERE request_date::DATE = '{date_str}'
            """
            
            actual_result = self._execute_query(actual_query)
            
            if actual_result.empty:
                raise StatsGenerationError(f"No data found for date {date_str}")
            
            actual_total = int(actual_result['total_permits'].iloc[0])
            actual_emergency = int(actual_result['emergency_permits'].iloc[0])
            actual_regular = int(actual_result['regular_permits'].iloc[0])
            
            # Get historical averages for this day of week
            avg_query = f"""
            WITH day_counts AS (
                SELECT
                    request_date::DATE as date,
                    COUNT(*) as total_permits,
                    SUM(CASE WHEN is_emergency::BOOLEAN THEN 1 ELSE 0 END) as emergency_permits,
                    SUM(CASE WHEN NOT is_emergency::BOOLEAN THEN 1 ELSE 0 END) as regular_permits
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
                WHERE request_date::DATE <= '{today}'
                AND request_date::DATE >= '{start_date}'
                AND DAYNAME(request_date::DATE) = '{day_of_week}'
                GROUP BY 1
            )
            SELECT
                AVG(total_permits) as avg_total,
                AVG(emergency_permits) as avg_emergency,
                AVG(regular_permits) as avg_regular,
                COUNT(*) as num_days
            FROM day_counts
            """
            
            avg_result = self._execute_query(avg_query)
            
            if avg_result.empty or avg_result['num_days'].iloc[0] == 0:
                raise StatsGenerationError(f"No historical data found for {day_of_week}")
            
            avg_total = float(avg_result['avg_total'].iloc[0])
            avg_emergency = float(avg_result['avg_emergency'].iloc[0])
            avg_regular = float(avg_result['avg_regular'].iloc[0])
            
            # Calculate percentage differences
            total_diff_percent = ((actual_total - avg_total) / avg_total * 100) if avg_total > 0 else 0
            emergency_diff_percent = ((actual_emergency - avg_emergency) / avg_emergency * 100) if avg_emergency > 0 else 0
            regular_diff_percent = ((actual_regular - avg_regular) / avg_regular * 100) if avg_regular > 0 else 0
            
            comparison = {
                'day_name': day_of_week,
                'actual_total': actual_total,
                'actual_emergency': actual_emergency,
                'actual_regular': actual_regular,
                'avg_total': round(avg_total, 1),
                'avg_emergency': round(avg_emergency, 1),
                'avg_regular': round(avg_regular, 1),
                'total_diff_percent': round(total_diff_percent, 1),
                'emergency_diff_percent': round(emergency_diff_percent, 1),
                'regular_diff_percent': round(regular_diff_percent, 1)
            }
            
            logger.info("Day of week comparison generated successfully")
            logger.debug(f"Comparison data: {comparison}")
            
            return comparison
            
        except Exception as e:
            error_msg = f"Failed to generate day of week comparison: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)

    def get_contractor_leaderboard(self, limit: int = 10) -> Dict[str, List[Dict]]:
        """Generate leaderboards for contractors."""
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
                AND contact_last_name != ''
                AND contact_last_name IS NOT NULL
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
            
            # First pass to get normalized names and their counts
            name_counts = {}
            for category_records in result:
                if not category_records:
                    continue
                for record in category_records:
                    parsed = self._parse_record(record)
                    if parsed:
                        name = parsed['name']
                        count = parsed['count']
                        if name not in name_counts:
                            name_counts[name] = {'total': 0, 'emergency': 0, 'streets': 0}
                        name_counts[name]['total'] = max(name_counts[name]['total'], count)
                        name_counts[name]['emergency'] = max(name_counts[name]['emergency'], count)
                        name_counts[name]['streets'] = max(name_counts[name]['streets'], count)
            
            # Second pass to create sorted leaderboards
            leaderboards = {
                'overall': [],
                'emergency': [],
                'streets': []
            }
            
            # Sort by total tickets
            sorted_by_total = sorted(name_counts.items(), key=lambda x: x[1]['total'], reverse=True)
            leaderboards['overall'] = [{'name': name, 'count': stats['total']} 
                                     for name, stats in sorted_by_total[:limit]]
            
            # Sort by emergency tickets
            sorted_by_emergency = sorted(name_counts.items(), key=lambda x: x[1]['emergency'], reverse=True)
            leaderboards['emergency'] = [{'name': name, 'count': stats['emergency']} 
                                       for name, stats in sorted_by_emergency[:limit]]
            
            # Sort by unique streets
            sorted_by_streets = sorted(name_counts.items(), key=lambda x: x[1]['streets'], reverse=True)
            leaderboards['streets'] = [{'name': name, 'count': stats['streets']} 
                                     for name, stats in sorted_by_streets[:limit]]
            
            logger.info("Contractor leaderboard generated successfully")
            logger.debug(f"Top contractor: {leaderboards['overall'][0] if leaderboards['overall'] else None}")
            
            return leaderboards
            
        except Exception as e:
            error_msg = f"Failed to generate contractor leaderboard: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)
            
    def __del__(self):
        """Cleanup database connection on object destruction."""
        try:
            if hasattr(self, 'db'):
                self.db.close()
                logger.debug("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}")
