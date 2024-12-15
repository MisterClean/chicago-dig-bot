"""Script to post a random permit's street view image to Bluesky."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import pandas as pd
import random
import glob
from datetime import datetime, timedelta
import logging
import os
from src.utils.property_image import PropertyImageBot
from src.social.bluesky import BlueskyPoster
from src.utils.logging import get_logger, setup_logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
setup_logging()
logger = get_logger(__name__)
logger.setLevel(logging.INFO)

def get_random_permit_from_yesterday():
    """Get a random permit that started digging yesterday from parquet files."""
    try:
        # Get yesterday's date
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Looking for permits from {yesterday}")
        
        # Find latest parquet file
        parquet_files = glob.glob('data/chicago811_*.parquet')
        if not parquet_files:
            logger.error("No parquet files found")
            return None
            
        latest_parquet = max(parquet_files)
        logger.info(f"Reading from {latest_parquet}")
        
        # Read parquet file
        df = pd.read_parquet(latest_parquet)
        logger.info(f"Loaded {len(df)} total permits")
        
        # Filter for permits that started yesterday
        df['dig_date'] = pd.to_datetime(df['dig_date'])
        yesterday_permits = df[df['dig_date'].dt.strftime('%Y-%m-%d') == yesterday]
        logger.info(f"Found {len(yesterday_permits)} permits from yesterday")
        
        if len(yesterday_permits) == 0:
            logger.warning(f"No permits found for {yesterday}")
            return None
            
        # Pick a random permit
        permit = yesterday_permits.sample(n=1).iloc[0]
        logger.info(f"Selected permit: {permit['ticket_number']}")
        
        # Format address
        address = f"{int(permit['street_number_from'])} {permit['street_direction']} {permit['street_name']} {permit['street_suffix']}, Chicago, IL"
        logger.info(f"Formatted address: {address}")
        
        # Format work type (replace underscores with commas)
        work_type = permit['dig_location'].strip() if pd.notna(permit['dig_location']) else "General Construction"
        work_type = work_type.replace('_', ',')
        if not work_type:
            work_type = "General Construction"
        
        return {
            'application_number': permit['ticket_number'],
            'work_type': work_type,
            'address': address,
            'is_emergency': permit['is_emergency']
        }
        
    except Exception as e:
        logger.error(f"Error reading parquet files: {str(e)}")
        return None

def main():
    """Main function to post a random permit's street view image."""
    image_path = None
    try:
        # Debug: Print environment variables
        logger.info(f"BLUESKY_HANDLE: {os.getenv('BLUESKY_HANDLE')}")
        
        logger.info("Starting random permit post process")
        
        # Get random permit
        permit = get_random_permit_from_yesterday()
        if not permit:
            logger.error("No permit found to post")
            return
            
        logger.info(f"Processing permit {permit['application_number']}")
        
        # Get street view image
        image_bot = PropertyImageBot()
        image_result = image_bot.process_address(permit['address'])
        
        if not image_result or image_result.get('status') != 'success':
            logger.error("Failed to get street view image")
            return
            
        image_path = image_result['image_path']
        logger.info(f"Successfully got street view image: {image_path}")
        
        # Format post text
        post_text = "🎲 Hole Roulette!\n\n"
        post_text += "Here's a permit with a Dig Date yesterday\n\n"
        post_text += f"📍 {permit['address']}\n"
        post_text += f"🔧 {permit['work_type']}\n"
        post_text += f"📝 Permit #{permit['application_number']}"
        if permit['is_emergency']:
            post_text += "\n🚨 Emergency Work"
            
        logger.info("Post text formatted:")
        logger.info(post_text)
        
        # Post to Bluesky
        bluesky = BlueskyPoster()
        bluesky.post_thread([{
            'text': post_text,
            'image': image_path,
            'alt': f"Google Street View image of {permit['address']}"
        }])
        
        logger.info("Successfully posted random permit")
        
        # Clean up the temporary image file after successful post
        if image_path and os.path.exists(image_path):
            os.remove(image_path)
            logger.info(f"Cleaned up temporary image file: {image_path}")
            
            # Also clean up emergency heatmap if it exists
            heatmap_path = 'output/emergency_heatmap.png'
            if os.path.exists(heatmap_path):
                os.remove(heatmap_path)
                logger.info("Cleaned up emergency heatmap")
        
    except Exception as e:
        logger.error(f"Error in post_random_permit: {str(e)}")
    finally:
        # Ensure cleanup happens even if post fails
        if image_path and os.path.exists(image_path):
            try:
                os.remove(image_path)
                logger.info(f"Cleaned up temporary image file in finally block: {image_path}")
            except Exception as e:
                logger.error(f"Error cleaning up temporary file: {str(e)}")
            
            # Also try to clean up emergency heatmap
            try:
                heatmap_path = 'output/emergency_heatmap.png'
                if os.path.exists(heatmap_path):
                    os.remove(heatmap_path)
                    logger.info("Cleaned up emergency heatmap in finally block")
            except Exception as e:
                logger.error(f"Error cleaning up heatmap: {str(e)}")

if __name__ == "__main__":
    main()
