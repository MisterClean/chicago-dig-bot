"""Module for fetching property images from Google Street View."""
import requests
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
from datetime import datetime
import os
from pathlib import Path
from utils.logging import get_logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = get_logger(__name__)

class PropertyImageError(Exception):
    """Custom exception for property image fetching errors."""
    pass

class PropertyImageBot:
    def __init__(self):
        """Initialize the property image bot with geocoder and API key."""
        self.geolocator = Nominatim(
            user_agent="chicago_dig_bot",
            timeout=5  # Increase timeout to 5 seconds
        )
        self.api_key = os.getenv('GOOGLE_MAPS_API_KEY')
        if not self.api_key:
            raise PropertyImageError("GOOGLE_MAPS_API_KEY environment variable not found")
        
        # Ensure images directory exists
        self.images_dir = Path("output/images")
        self.images_dir.mkdir(parents=True, exist_ok=True)
        
    def get_street_view_image(self, lat: float, lon: float, address: str) -> str:
        """
        Fetches a Street View image for given coordinates using Google Street View Static API
        
        Args:
            lat: Latitude coordinate
            lon: Longitude coordinate
            address: Address string for filename
            
        Returns:
            Path to saved image file
            
        Raises:
            PropertyImageError: If image fetch fails
        """
        try:
            base_url = "https://maps.googleapis.com/maps/api/streetview"
            params = {
                'size': '600x400',  # Image size
                'location': f'{lat},{lon}',
                'key': self.api_key,
                'return_error_code': True
            }
            
            response = requests.get(f"{base_url}", params=params, timeout=10)  # Add 10 second timeout
            
            if response.status_code == 200:
                # Create filename from sanitized address
                safe_address = "".join(x for x in address if x.isalnum() or x in (' ', '-', '_'))
                filename = self.images_dir / f"{safe_address}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
                
                with open(filename, 'wb') as f:
                    f.write(response.content)
                return str(filename)
            else:
                raise PropertyImageError(f"Failed to fetch Street View image: {response.status_code}")
                
        except Exception as e:
            raise PropertyImageError(f"Error fetching Street View image: {str(e)}")

    def process_address(self, address: str) -> dict:
        """
        Geocodes an address and fetches its Street View image
        
        Args:
            address: Address string to process
            
        Returns:
            Dictionary containing status, address, coordinates and image path
            
        Raises:
            PropertyImageError: If processing fails
        """
        try:
            logger.info(f"Processing address: {address}")
            
            # Geocode the address with retry logic
            max_retries = 3
            retry_delay = 1  # seconds
            
            for attempt in range(max_retries):
                try:
                    location = self.geolocator.geocode(address)
                    if location:
                        logger.info(f"Successfully geocoded address: {location.latitude}, {location.longitude}")
                        break
                    elif attempt < max_retries - 1:  # Don't sleep on last attempt
                        time.sleep(retry_delay)
                except GeocoderTimedOut:
                    if attempt < max_retries - 1:  # Don't sleep on last attempt
                        time.sleep(retry_delay)
                    continue
            
            if location:
                # Fetch and save street view image
                image_path = self.get_street_view_image(
                    location.latitude, 
                    location.longitude,
                    address
                )
                
                return {
                    'status': 'success',
                    'address': address,
                    'lat': location.latitude,
                    'lon': location.longitude,
                    'image_path': image_path
                }
            else:
                raise PropertyImageError(f"Could not geocode address: {address}")
                
        except GeocoderTimedOut:
            raise PropertyImageError("Geocoding timed out after all retries")
        except Exception as e:
            raise PropertyImageError(f"Error processing address: {str(e)}")
