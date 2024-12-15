"""Tests for heatmap visualization functionality."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.visualization.charts import ChartGenerator

def test_heatmap_with_recent_data():
    """Test heatmap generation with recent dig dates."""
    # Create test data
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    two_days_ago = today - timedelta(days=2)
    
    df = pd.DataFrame({
        'latitude': [41.8781, 41.8782, 41.8783],
        'longitude': [-87.6298, -87.6299, -87.6300],
        'dig_date': [
            yesterday,  # Should be included
            two_days_ago,  # Should be excluded
            today  # Should be included
        ],
        'is_emergency': [True, False, False]
    })
    
    # Initialize chart generator
    generator = ChartGenerator()
    
    # Create output directory if it doesn't exist
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    # Generate emergency heatmap
    emergency_output = str(output_dir / "test_emergency_heatmap.png")
    generator.create_emergency_heatmap(df, emergency_output)
    
    # Verify file was created
    assert Path(emergency_output).exists()

if __name__ == "__main__":
    test_heatmap_with_recent_data()
