"""Tests for Bluesky posting functionality."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from src.social.bluesky import BlueskyPoster
import pandas as pd
from datetime import datetime

def test_daily_thread():
    """Test daily thread post with sample data."""
    # Create sample day comparison data
    day_comparison = {
        'day_name': 'Monday',
        'actual_total': 150,
        'actual_emergency': 15,
        'actual_regular': 135,
        'expected_total': 140,
        'expected_emergency': 12,
        'expected_regular': 128,
        'total_diff_percent': 7.14,
        'emergency_diff_percent': 25.0,
        'regular_diff_percent': 5.47
    }

    # Create sample leaderboard
    leaderboard = {
        'overall': [
            {'name': 'ABC Construction', 'count': 500},
            {'name': 'XYZ Diggers', 'count': 450},
            {'name': 'City Works', 'count': 400},
            {'name': 'Metro Utilities', 'count': 350},
            {'name': 'Urban Excavators', 'count': 300}
        ]
    }

    # Create sample permit stats
    permit_stats = {
        'total_count': 150,
        'emergency_count': 15,
        'regular_count': 135,
        'emergency_percent': 10.0
    }

    # Initialize poster in test mode
    poster = BlueskyPoster()

    # Test thread post
    print("\n=== Testing Daily Thread Post ===")
    poster.post_daily_thread(
        day_comparison=day_comparison,
        leaderboard=leaderboard,
        permit_stats=permit_stats,
        heatmap_path="output/test_emergency_heatmap.png",
        emergency_heatmap_path="output/test_emergency_heatmap.png"
    )

if __name__ == "__main__":
    test_daily_thread()
