"""Test script for stats generation."""
from analytics.stats import StatsGenerator
from utils.logging import get_logger
from datetime import datetime, timedelta
import pytz
import argparse

logger = get_logger(__name__)

def format_comparison_output(day_comparison):
    """Format the day comparison data for display."""
    output = []
    output.append(f"\n📊 Day of Week Comparison for {day_comparison['day_name']}")
    output.append("\nActual vs Historical Average:")
    
    # Total permits
    output.append(f"Total Permits:     {day_comparison['actual_total']} vs {day_comparison['avg_total']:.1f}")
    diff_symbol = '⬇️' if day_comparison['total_diff_percent'] < 0 else '⬆️'
    output.append(f"                   {diff_symbol} {abs(day_comparison['total_diff_percent']):.1f}% vs {day_comparison['day_name']} average")
    
    # Emergency permits
    output.append(f"\nEmergency Permits: {day_comparison['actual_emergency']} vs {day_comparison['avg_emergency']:.1f}")
    diff_symbol = '⬇️' if day_comparison['emergency_diff_percent'] < 0 else '⬆️'
    output.append(f"                   {diff_symbol} {abs(day_comparison['emergency_diff_percent']):.1f}% vs {day_comparison['day_name']} average")
    
    # Regular permits
    output.append(f"\nRegular Permits:   {day_comparison['actual_regular']} vs {day_comparison['avg_regular']:.1f}")
    diff_symbol = '⬇️' if day_comparison['regular_diff_percent'] < 0 else '⬆️'
    output.append(f"                   {diff_symbol} {abs(day_comparison['regular_diff_percent']):.1f}% vs {day_comparison['day_name']} average")
    
    return "\n".join(output)

def format_leaderboard_output(leaderboard):
    """Format the leaderboard data for display."""
    output = []
    output.append("\n🏆 Contractor Leaderboards:")
    
    # Overall leaders
    output.append("\nTop Overall:")
    for entry in leaderboard['overall']:
        output.append(f"- {entry.get('name', 'Unknown')}: {entry.get('count', 0)}")
    
    # Emergency leaders    
    output.append("\nTop Emergency:")
    for entry in leaderboard['emergency']:
        output.append(f"- {entry.get('name', 'Unknown')}: {entry.get('count', 0)}")
    
    # Streets leaders    
    output.append("\nTop Streets:")
    for entry in leaderboard['streets']:
        output.append(f"- {entry.get('name', 'Unknown')}: {entry.get('count', 0)}")
    
    return "\n".join(output)

def main():
    """Test stats generation."""
    parser = argparse.ArgumentParser(description='Test stats generation')
    parser.add_argument('--date', type=str, help='Date to analyze in YYYY-MM-DD format')
    args = parser.parse_args()
    
    try:
        stats = StatsGenerator()
        
        # Use provided date or default to yesterday
        chicago_tz = pytz.timezone('America/Chicago')
        chicago_now = datetime.now(chicago_tz)
        test_date = args.date if args.date else (chicago_now - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"\nAnalyzing data for: {test_date}")
        
        try:
            # Get day comparison
            day_comparison = stats.get_day_of_week_comparison(test_date)
            print(format_comparison_output(day_comparison))
        except Exception as e:
            logger.error(f"Failed to get day comparison: {str(e)}")
            print(f"\n⚠️  Could not generate day comparison: {str(e)}")
        
        try:
            # Get leaderboard
            leaderboard = stats.get_contractor_leaderboard(limit=5)
            print(format_leaderboard_output(leaderboard))
        except Exception as e:
            logger.error(f"Failed to get leaderboard: {str(e)}")
            print(f"\n⚠️  Could not generate leaderboard: {str(e)}")
            
    except Exception as e:
        logger.error(f"Error testing stats: {str(e)}")
        raise

if __name__ == "__main__":
    main()
