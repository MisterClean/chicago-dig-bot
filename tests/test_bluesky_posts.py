"""Test script to show what Bluesky posts would look like in test mode using real data."""
from src.social.bluesky import BlueskyPoster
from src.data.storage import DataStorage
from src.analytics.stats import StatsGenerator
from src.visualization.charts import ChartGenerator
from datetime import datetime, timedelta
import logging
from pathlib import Path
import pandas as pd

# Configure logging to print to stdout
logging.basicConfig(level=logging.INFO, format='%(message)s')

def main():
    # Initialize components
    storage = DataStorage()
    stats = StatsGenerator()
    charts = ChartGenerator()
    poster = BlueskyPoster()

    # Load existing data from parquet
    print("\n=== Loading existing data ===")
    parquet_file = list(Path('data').glob('chicago811_*.parquet'))[0]
    df = pd.read_parquet(parquet_file)
    print(f"Loaded {len(df)} records from {parquet_file}")

    # Generate day-of-week comparison
    print("\n=== Generating day-of-week comparison ===")
    # Use yesterday's date since today's data might not be complete
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    day_comparison = stats.get_day_of_week_comparison(yesterday)
    print(f"Day of week comparison: {day_comparison}")

    # If no comparison data available, use sample data for testing
    if not day_comparison:
        print("Using sample data for testing since no comparison data available")
        day_comparison = {
            'day_name': 'Friday',
            'actual_total': 100,
            'actual_emergency': 20,
            'actual_regular': 80,
            'expected_total': 90,
            'expected_emergency': 15,
            'expected_regular': 75,
            'total_diff_percent': 11.11,
            'emergency_diff_percent': 33.33,
            'regular_diff_percent': 6.67
        }

    # Get contractor leaderboard
    print("\n=== Generating contractor leaderboard ===")
    leaderboard = stats.get_contractor_leaderboard(limit=5)
    print(f"Top contractor: {leaderboard['overall'][0]}")

    # Calculate permit stats
    print("\n=== Calculating permit statistics ===")
    permit_stats = {
        'total_count': day_comparison['actual_total'],
        'emergency_count': day_comparison['actual_emergency'],
        'regular_count': day_comparison['actual_total'] - day_comparison['actual_emergency'],
        'emergency_percent': round((day_comparison['actual_emergency'] / day_comparison['actual_total']) * 100, 1)
    }
    print(f"Permit stats: {permit_stats}")

    # Create heatmaps
    print("\n=== Generating heatmaps ===")
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    emergency_heatmap_path = str(output_dir / 'test_emergency_heatmap.png')
    charts.create_emergency_heatmap(df, emergency_heatmap_path)
    print(f"Created emergency heatmap at {emergency_heatmap_path}")

    print("\n=== Testing Thread Post with Real Data ===")
    
    # Format thread posts
    thread_posts = []
    
    # 1. Summary post comparing to historical day-of-week average
    summary_text = f"📊 Chicago Dig Report - {day_comparison['day_name']}\n\n"
    summary_text += f"Total Permits: {day_comparison['actual_total']} "
    if day_comparison['total_diff_percent'] > 0:
        summary_text += f"(⬆️ {day_comparison['total_diff_percent']}% vs avg)"
    else:
        summary_text += f"(⬇️ {abs(day_comparison['total_diff_percent'])}% vs avg)"
    summary_text += f"\n\nEmergency Permits: {day_comparison['actual_emergency']} "
    if day_comparison['emergency_diff_percent'] > 0:
        summary_text += f"(⬆️ {day_comparison['emergency_diff_percent']}% vs avg)"
    else:
        summary_text += f"(⬇️ {abs(day_comparison['emergency_diff_percent'])}% vs avg)"
        
    thread_posts.append({'text': summary_text})
    
    # 2. Top diggers leaderboard
    leaders = leaderboard['overall'][:5]  # Top 5 for readability
    leaderboard_text = "🏆 Today's Top Diggers\n\n"
    for i, leader in enumerate(leaders, 1):
        emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👏"
        leaderboard_text += f"{emoji} {leader['name']}: {leader['count']}\n"
        
    thread_posts.append({'text': leaderboard_text})
    
    # 3. Emergency vs normal permits with heatmap
    permit_text = f"📍 Total Permits: {permit_stats['total_count']}\n\n"
    permit_text += f"Emergency Permits: {permit_stats['emergency_count']} "
    if day_comparison['emergency_diff_percent'] > 0:
        permit_text += f"⬆️{day_comparison['emergency_diff_percent']}% vs {day_comparison['day_name']} avg\n\n"
    else:
        permit_text += f"⬇️{abs(day_comparison['emergency_diff_percent'])}% vs {day_comparison['day_name']} avg\n\n"
    
    permit_text += f"Regular Permits: {permit_stats['regular_count']} "
    if day_comparison['regular_diff_percent'] > 0:
        permit_text += f"⬆️{day_comparison['regular_diff_percent']}% vs {day_comparison['day_name']} avg"
    else:
        permit_text += f"⬇️{abs(day_comparison['regular_diff_percent'])}% vs {day_comparison['day_name']} avg"
    
    thread_posts.append({
        'text': permit_text,
        'image': emergency_heatmap_path,
        'alt': f"A map of Chicago showing the distribution of dig permits. Emergency permits are shown in orange, regular permits in blue. {permit_text}"
    })
    
    # Post the thread
    print("\n=== Simulating thread post ===")
    poster.post_thread(thread_posts)

    print("\n=== Testing Legacy Post Formats ===")
    
    # Now generate stats from the stored data
    print("\n=== Generating statistics ===")
    daily_stats = stats.generate_daily_stats()
    chart_path, latest_stats = charts.create_daily_chart(daily_stats)

    # Get records from stats
    records = stats.get_daily_records()

    print("\n=== Testing Daily Update Post with Real Data ===")
    poster._make_post(config.bluesky_post_template.format(
        total_tickets=latest_stats['total_tickets'],
        emergency_tickets=latest_stats['emergency_tickets'],
        emergency_percent=latest_stats['emergency_percent'],
        regular_tickets=latest_stats['regular_tickets']
    ), chart_path)

    print("\n=== Testing Records Post with Real Data ===")
    post_text = f"📊 Chicago Dig Records!\n\n"
    post_text += f"Most dig tickets in one day: {records['most_tickets']['count']} on {records['most_tickets']['name']}\n"
    post_text += f"Most emergency tickets: {records['most_emergency']['count']} on {records['most_emergency']['name']}"
    poster._make_post(post_text)

    print("\n=== Testing Leaderboard Post with Real Data ===")
    leaders = leaderboard['overall']
    post_text = "👑 Top Diggers Leaderboard\n\n"
    for i, leader in enumerate(leaders, 1):
        emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "👏"
        post_text += f"{emoji} {leader['name']}: {leader['count']}\n"
    poster._make_post(post_text)

if __name__ == "__main__":
    from src.config import config
    main()
