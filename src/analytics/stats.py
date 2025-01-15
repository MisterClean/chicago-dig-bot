"""Module for generating statistics and analytics from Chicago 811 dig ticket data."""
import pandas as pd
import duckdb
from typing import Dict, List, Optional
from pathlib import Path
from src.config import config
from src.utils.logging import get_logger
import re
from datetime import datetime, timedelta
import pytz

logger = get_logger(__name__)

class StatsGenerationError(Exception):
    """Custom exception for statistics generation errors."""
    pass

class StatsGenerator:
    """Handles generation of statistics and analytics from dig ticket data."""
    
    # Common business suffixes to normalize
    BUSINESS_SUFFIXES = {
        r'(?:,\s+)?INC(?:ORPORATED)?\.?$': 'Inc',
        r'(?:,\s+)?LLC\.?$': 'LLC',
        r'(?:,\s+)?CO(?:MPANY)?\.?$': 'Co',
        r'(?:,\s+)?CORP(?:ORATION)?\.?$': 'Corp',
        r'(?:,\s+)?LTD\.?$': 'Ltd',
    }
    
    # Common word replacements - ordered by specificity
    WORD_REPLACEMENTS = {
        r'CONSTRUCTION': 'Construction',
        r'NSTRUCTION': 'Construction',
        r'CONSTR\.?': 'Construction',
        r'CONST\.?': 'Construction',
        r'PLBG\.?': 'Plumbing',
        r'HTG\.?': 'Heating',
        r'EXCAV\.?': 'Excavating',
        r'CONCRETE': 'Concrete',
        r'NCRETE': 'Concrete',
    }
    
    # Known name variations to normalize
    NAME_MAPPINGS = {
        # Utilities
        "PEOPLES GAS": "Peoples Gas",
        "PEOPLE GAS": "Peoples Gas",
        "PEOPLES GAS LIGHT & COKE": "Peoples Gas",
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
        "CHICAGO DEPT WATER MANAGEMENT": "Department of Water Management",
        "DEPT OF WATER MANAGEMENT": "Department of Water Management",
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
        
        # Extract business suffix if present
        business_suffix = ''
        original_name = name  # Keep original for comparison
        for suffix_pattern, replacement in self.BUSINESS_SUFFIXES.items():
            match = re.search(suffix_pattern, name, re.IGNORECASE)
            if match:
                business_suffix = f" {replacement}"
                name = re.sub(suffix_pattern, '', name, flags=re.IGNORECASE).strip()
                break
        
        # Normalize ampersands and other conjunctions
        name = name.replace(' AND ', ' & ')
        name = name.replace('&', ' & ')  # Add spaces around ampersands
        name = re.sub(r'\s+', ' ', name)  # Clean up any resulting double spaces
        
        # Apply word replacements for common abbreviations
        words = name.split()
        normalized_words = []
        for word in words:
            # Check if the word matches any of our replacement patterns
            replaced = False
            for pattern, replacement in self.WORD_REPLACEMENTS.items():
                if re.match(f"^{pattern}$", word):
                    normalized_words.append(replacement)
                    replaced = True
                    break
            if not replaced:
                normalized_words.append(word)
        name = ' '.join(normalized_words)
        
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
        
        # Only add back business suffix if it was present in original
        if business_suffix and any(re.search(pattern, original_name, re.IGNORECASE) 
                                 for pattern in self.BUSINESS_SUFFIXES.keys()):
            name = f"{name}{business_suffix}"
        
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
        """Generate basic statistics for the current day's permits."""
        try:
            logger.info("Generating daily statistics")
            self._validate_parquet_files()
            
            chicago_tz = pytz.timezone('America/Chicago')
            chicago_now = datetime.now(chicago_tz)
            yesterday = (chicago_now - timedelta(days=1)).date()
            
            # Query using DuckDB's date handling
            query = f"""
            WITH chicago_times AS (
                SELECT *,
                    dig_date::TIMESTAMP AT TIME ZONE 'America/Chicago' AS chicago_time
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
            )
            SELECT
                CAST(COUNT(*) AS INTEGER) as total_permits,
                CAST(SUM(CASE WHEN is_emergency::BOOLEAN THEN 1 ELSE 0 END) AS INTEGER) as emergency_permits,
                CAST(COUNT(DISTINCT street_name) AS INTEGER) as unique_streets
            FROM chicago_times
            WHERE chicago_time::DATE = '{yesterday}'
            """
            
            result = self._execute_query(query)
            
            if result.empty:
                logger.warning(f"No data found for {yesterday}")
                return {
                    'total_permits': 0,
                    'emergency_permits': 0,
                    'regular_permits': 0,
                    'unique_streets': 0
                }
            
            # Extract values and handle nulls safely
            total_permits = int(result['total_permits'].iloc[0] or 0)
            emergency_permits = int(result['emergency_permits'].iloc[0] or 0)
            unique_streets = int(result['unique_streets'].iloc[0] or 0)
            
            stats = {
                'total_permits': total_permits,
                'emergency_permits': emergency_permits,
                'regular_permits': total_permits - emergency_permits,
                'unique_streets': unique_streets
            }
            
            logger.info(f"Generated daily statistics: {stats}")
            return stats
            
        except Exception as e:
            error_msg = f"Failed to generate daily statistics: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)

    def get_day_of_week_comparison(self, date_str: str) -> Dict:
        """Compare permit counts for a given date with historical averages."""
        try:
            logger.info(f"Generating day of week comparison for {date_str}")
            
            # Parse the date
            chicago_tz = pytz.timezone('America/Chicago')
            target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            day_of_week = target_date.strftime('%A')
            
            # Get current date range for historical comparison
            rolling_window = config._get_nested('analytics', 'stats', 'rolling_window_days')
            history_start = target_date - timedelta(days=rolling_window)
            
            # Query using DuckDB's date handling
            query = f"""
            WITH chicago_times AS (
                SELECT *,
                    dig_date::TIMESTAMP AT TIME ZONE 'America/Chicago' AS chicago_time
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
            ),
            daily_counts AS (
                SELECT 
                    chicago_time::DATE as date,
                    COUNT(*) as total_permits,
                    SUM(CASE WHEN is_emergency::BOOLEAN THEN 1 ELSE 0 END) as emergency_permits
                FROM chicago_times
                WHERE chicago_time::DATE = '{target_date}'
                GROUP BY 1
            )
            SELECT 
                COALESCE(total_permits, 0) as total_permits,
                COALESCE(emergency_permits, 0) as emergency_permits
            FROM daily_counts
            """
            
            result = self._execute_query(query)
            
            # Get actual counts
            if result.empty:
                actual_total = 0
                actual_emergency = 0
            else:
                actual_total = int(result['total_permits'].iloc[0])
                actual_emergency = int(result['emergency_permits'].iloc[0])
            
            actual_regular = actual_total - actual_emergency
            
            # Get historical averages with timezone-aware comparison
            avg_query = f"""
            WITH chicago_times AS (
                SELECT *,
                    dig_date::TIMESTAMP AT TIME ZONE 'America/Chicago' AS chicago_time
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
            ),
            historical_counts AS (
                SELECT 
                    chicago_time::DATE as date,
                    COUNT(*) as total_permits,
                    SUM(CASE WHEN is_emergency::BOOLEAN THEN 1 ELSE 0 END) as emergency_permits
                FROM chicago_times
                WHERE DAYNAME(chicago_time::DATE) = '{day_of_week}'
                AND chicago_time::DATE < '{target_date}'
                AND chicago_time::DATE >= '{history_start}'
                GROUP BY 1
            )
            SELECT
                AVG(total_permits) as avg_total,
                AVG(emergency_permits) as avg_emergency,
                COUNT(*) as num_days
            FROM historical_counts
            """
            
            avg_result = self._execute_query(avg_query)
            
            if avg_result.empty or int(avg_result['num_days'].iloc[0] or 0) == 0:
                avg_total = 0
                avg_emergency = 0
            else:
                avg_total = float(avg_result['avg_total'].iloc[0] or 0)
                avg_emergency = float(avg_result['avg_emergency'].iloc[0] or 0)
            
            avg_regular = avg_total - avg_emergency
            
            # Calculate percent differences
            def calc_percent_diff(actual: int, avg: float) -> float:
                return round(((actual - avg) / avg * 100) if avg > 0 else 0, 1)
            
            comparison = {
                'day_name': day_of_week,
                'actual_total': actual_total,
                'actual_emergency': actual_emergency,
                'actual_regular': actual_regular,
                'avg_total': round(avg_total, 1),
                'avg_emergency': round(avg_emergency, 1),
                'avg_regular': round(avg_regular, 1),
                'total_diff_percent': calc_percent_diff(actual_total, avg_total),
                'emergency_diff_percent': calc_percent_diff(actual_emergency, avg_emergency),
                'regular_diff_percent': calc_percent_diff(actual_regular, avg_regular)
            }
            
            logger.info(f"Generated day comparison: {comparison}")
            return comparison
            
        except Exception as e:
            error_msg = f"Failed to generate day comparison: {str(e)}"
            logger.error(error_msg)
            raise StatsGenerationError(error_msg)

    def get_contractor_leaderboard(self, limit: int = 5) -> List[Dict]:
        """Generate leaderboards for contractors."""
        try:
            logger.info(f"Generating contractor leaderboard (limit: {limit})")
            
            chicago_tz = pytz.timezone('America/Chicago')
            chicago_now = datetime.now(chicago_tz)
            yesterday = (chicago_now - timedelta(days=1)).date()
            
            # Debug current data
            debug_query = f"""
            WITH chicago_times AS (
                SELECT *,
                    dig_date::TIMESTAMP AT TIME ZONE 'America/Chicago' AS chicago_time
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
            )
            SELECT DISTINCT 
                contact_first_name, 
                contact_last_name,
                COUNT(*) as count
            FROM chicago_times
            WHERE chicago_time::DATE = '{yesterday}'
            GROUP BY contact_first_name, contact_last_name
            ORDER BY count DESC
            LIMIT 10
            """
            
            debug_result = self._execute_query(debug_query)
            logger.debug(f"Debug contractor data:\n{debug_result}")
            
            # Main query with contractor name handling
            query = f"""
            WITH chicago_times AS (
                SELECT *,
                    dig_date::TIMESTAMP AT TIME ZONE 'America/Chicago' AS chicago_time
                FROM read_parquet('{str(config.data_dir)}/*.parquet')
            ),
            contractor_counts AS (
                SELECT 
                    CASE 
                        WHEN contact_first_name = '' OR contact_first_name IS NULL THEN contact_last_name
                        ELSE contact_first_name || ' ' || contact_last_name 
                    END as contractor_name,
                    COUNT(*) as permit_count
                FROM chicago_times
                WHERE chicago_time::DATE = '{yesterday}'
                AND contact_last_name IS NOT NULL 
                AND contact_last_name != ''
                GROUP BY 1
                HAVING COUNT(*) > 0
                ORDER BY permit_count DESC
                LIMIT {limit}
            )
            SELECT * FROM contractor_counts
            """
            
            result = self._execute_query(query)
            
            if result.empty:
                logger.warning("No contractor data found for leaderboard")
                return []
            
            # Process results and normalize names
            leaderboard = []
            for _, row in result.iterrows():
                name = self._normalize_name(row['contractor_name'])
                if name:  # Only add if we have a valid name
                    leaderboard.append({
                        'name': name,
                        'count': int(row['permit_count'])
                    })
            
            # Sort by count descending
            leaderboard = sorted(leaderboard, key=lambda x: x['count'], reverse=True)
            
            logger.info(f"Generated leaderboard with {len(leaderboard)} entries")
            logger.debug(f"Leaderboard: {leaderboard}")
            
            return leaderboard[:limit]
            
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
