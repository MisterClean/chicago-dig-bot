"""Tests for the data pipeline functionality."""
import pytest
import pandas as pd
from pathlib import Path
import shutil
from datetime import datetime, timedelta
import json
from src.data.fetcher import DataFetcher
from src.data.storage import DataStorage
from src.scripts.refresh_data import clean_data_directory

class TestConfig:
    """Test configuration class that allows setting values."""
    def __init__(self):
        self._data_dir = None
        self._initial_csv_path = None
        self.soda_days_to_fetch = 30
        self.full_refresh_days = 365
        self.soda_api_url = "https://data.cityofchicago.org/resource/66wz-dkef.json"
        
        # Database settings
        self._db_file = None
        self._db_backup_enabled = False
        self._db_backup_dir = None
        self._db_backup_retention = 7
        
    @property
    def data_dir(self):
        return self._data_dir
        
    @data_dir.setter
    def data_dir(self, value):
        self._data_dir = value
        # Set db_file and backup_dir relative to data_dir
        self._db_file = str(Path(value) / "chicago811.db")
        self._db_backup_dir = Path(value) / "backups"
        
    @property
    def initial_csv_path(self):
        return self._initial_csv_path
        
    @initial_csv_path.setter
    def initial_csv_path(self, value):
        self._initial_csv_path = value
        
    @property
    def db_file(self):
        return self._db_file
        
    @property
    def db_backup_enabled(self):
        return self._db_backup_enabled
        
    @property
    def db_backup_dir(self):
        return self._db_backup_dir
        
    @property
    def db_backup_retention(self):
        return self._db_backup_retention

@pytest.fixture
def setup_test_data(monkeypatch):
    """Setup test data directory and cleanup after tests."""
    # Create test data directory
    test_data_dir = Path("test_data")
    test_data_dir.mkdir(exist_ok=True)
    
    # Create test CSV data
    test_csv = test_data_dir / "test_data.csv"
    pd.DataFrame({
        'PERMIT#': ['TEST001', 'TEST002'],
        'EMERGENCY': ['Y', 'N'],
        'REQUESTDATE': ['2024-01-01', '2024-01-02'],
        'DIGDATE': ['2024-01-02', '2024-01-03'],
        'LATITUDE': [41.8781, 41.8782],
        'LONGITUDE': [-87.6298, -87.6299]
    }).to_csv(test_csv, index=False)
    
    # Create and inject test config
    test_config = TestConfig()
    test_config.data_dir = str(test_data_dir)
    test_config.initial_csv_path = str(test_csv)
    
    # Patch the config in all modules that use it
    import src.data.fetcher
    import src.data.storage
    import src.scripts.refresh_data
    
    monkeypatch.setattr(src.data.fetcher, 'config', test_config)
    monkeypatch.setattr(src.data.storage, 'config', test_config)
    monkeypatch.setattr(src.scripts.refresh_data, 'config', test_config)
    
    yield test_data_dir
    
    # Cleanup
    shutil.rmtree(test_data_dir)

def test_initial_csv_load(setup_test_data):
    """Test initial data load from CSV."""
    # Clean any existing data
    clean_data_directory()
    
    # Initialize components
    fetcher = DataFetcher()
    storage = DataStorage()
    
    # Fetch and store data
    data = fetcher.fetch_full_dataset()
    stats = storage.process_and_store(data)
    
    # Verify results
    assert len(data) == 2
    assert stats['total_records'] == 2
    assert stats['inserts'] == 2
    assert stats['updates'] == 0
    
    # Verify parquet file was created
    parquet_files = list(Path(setup_test_data).glob("chicago811_*.parquet"))
    assert len(parquet_files) == 1
    
    # Verify last_run_at was saved
    assert fetcher.state_file.exists()

def test_incremental_api_update(setup_test_data):
    """Test incremental update via API after initial load."""
    # First do initial load
    test_initial_csv_load(setup_test_data)
    
    # Mock API data by creating a new fetcher method
    original_fetch_recent = DataFetcher.fetch_recent_data
    try:
        def mock_fetch_recent(self, days=None):
            return pd.DataFrame({
                'dig_ticket_': ['TEST003', 'TEST004'],
                'emergency': [True, False],
                'requestdate': ['2024-01-03', '2024-01-04'],
                'digdate': ['2024-01-04', '2024-01-05'],
                'latitude': [41.8783, 41.8784],
                'longitude': [-87.6300, -87.6301]
            })
        
        DataFetcher.fetch_recent_data = mock_fetch_recent
        
        # Initialize components
        fetcher = DataFetcher()
        storage = DataStorage()
        
        # Fetch and store new data
        data = fetcher.fetch_full_dataset()
        stats = storage.process_and_store(data)
        
        # Verify results
        assert len(data) == 2  # Only new records
        assert stats['total_records'] == 4  # Total after merge
        assert stats['inserts'] == 2  # New records
        assert stats['updates'] == 0  # No updates to existing records
        
    finally:
        # Restore original method
        DataFetcher.fetch_recent_data = original_fetch_recent

def test_full_refresh(setup_test_data):
    """Test full refresh after cleaning data directory."""
    # First do initial load
    test_initial_csv_load(setup_test_data)
    
    # Clean data directory
    clean_data_directory()
    
    # Mock API data
    original_fetch_recent = DataFetcher.fetch_recent_data
    try:
        def mock_fetch_recent(self, days=None):
            return pd.DataFrame({
                'dig_ticket_': ['TEST005', 'TEST006'],
                'emergency': [True, False],
                'requestdate': ['2024-01-05', '2024-01-06'],
                'digdate': ['2024-01-06', '2024-01-07'],
                'latitude': [41.8785, 41.8786],
                'longitude': [-87.6302, -87.6303]
            })
        
        DataFetcher.fetch_recent_data = mock_fetch_recent
        
        # Initialize components
        fetcher = DataFetcher()
        storage = DataStorage()
        
        # Fetch and store data
        data = fetcher.fetch_full_dataset()
        stats = storage.process_and_store(data)
        
        # Verify results
        assert len(data) == 2
        assert stats['total_records'] == 2
        assert stats['inserts'] == 2
        assert stats['updates'] == 0
        
        # Verify only one parquet file exists
        parquet_files = list(Path(setup_test_data).glob("chicago811_*.parquet"))
        assert len(parquet_files) == 1
        
    finally:
        # Restore original method
        DataFetcher.fetch_recent_data = original_fetch_recent
