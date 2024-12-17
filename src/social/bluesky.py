"""Module for posting updates to Bluesky social network."""
from atproto import Client, client_utils
from atproto.exceptions import AtProtocolError
import pandas as pd
from typing import Dict, List, Optional
from pathlib import Path
import time
import os
import yaml
from utils.logging import get_logger
from dotenv import load_dotenv, find_dotenv
from datetime import datetime, timezone

# Load environment variables with override=True to ensure our values take precedence
load_dotenv(override=True)

logger = get_logger(__name__)

class BlueskyPostError(Exception):
    """Custom exception for Bluesky posting errors."""
    pass

class BlueskyPoster:
    """Handles posting updates to Bluesky social network."""
    
    def __init__(self):
        """Initialize Bluesky client with authentication."""
        self.client = None
        self._load_config()
        
        if not os.getenv('TEST_MODE', 'false').lower() == 'true':
            try:
                logger.info("Initializing Bluesky client")
                
                # Get credentials directly from environment
                handle = os.getenv('BLUESKY_HANDLE')
                password = os.getenv('BLUESKY_PASSWORD')
                
                # Debug environment variables
                logger.info(f"Environment file location: {find_dotenv()}")
                logger.info(f"Current working directory: {os.getcwd()}")
                logger.info(f"Using Bluesky handle: {handle}")
                
                if not handle or not password:
                    raise BlueskyPostError("Missing Bluesky credentials in environment")
                
                logger.info(f"Attempting to login with handle: {handle}")
                
                self.client = Client()
                self._login_with_retries(handle, password)
                
            except Exception as e:
                error_msg = f"Failed to initialize Bluesky client: {str(e)}"
                logger.error(error_msg)
                raise BlueskyPostError(error_msg)
        else:
            logger.info("Running in test mode - Bluesky client disabled")

    def _load_config(self):
        """Load configuration from yaml file."""
        try:
            with open("config.yaml", "r") as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {str(e)}")
            raise

    def _login_with_retries(self, handle: str, password: str):
        """Attempt to login with exponential backoff retries."""
        max_retries = self.config['errors']['retry']['max_attempts']
        initial_delay = self.config['errors']['retry']['delay_seconds']
        
        retries = 0
        delay = initial_delay
        
        while retries < max_retries:
            try:
                profile = self.client.login(handle, password)
                logger.info(f"Successfully authenticated with Bluesky as {profile.display_name}")
                return
            except Exception as e:
                logger.error(f"Login attempt {retries + 1} failed: {e}")
                if "RateLimitExceeded" in str(e) or "validation errors" in str(e):
                    time.sleep(delay)
                    if self.config['errors']['retry']['exponential_backoff']:
                        delay *= 2  # Exponential backoff
                    retries += 1
                else:
                    raise e
        
        raise BlueskyPostError(f"Max retries ({max_retries}) exceeded. Could not log in to Bluesky.")
            
    def _validate_image(self, image_path: str) -> None:
        """Validate image file exists and is accessible.
        
        Args:
            image_path: Path to the image file.
            
        Raises:
            BlueskyPostError: If image validation fails.
        """
        path = Path(image_path)
        if not path.exists():
            error_msg = f"Image file not found: {image_path}"
            logger.error(error_msg)
            raise BlueskyPostError(error_msg)
            
        if not path.is_file():
            error_msg = f"Image path is not a file: {image_path}"
            logger.error(error_msg)
            raise BlueskyPostError(error_msg)

    def _upload_image(self, image_path: str) -> Dict:
        """Upload an image and return its blob reference.
        
        Args:
            image_path: Path to the image file.
            
        Returns:
            Dict containing the uploaded image blob reference.
            
        Raises:
            BlueskyPostError: If image upload fails.
        """
        try:
            with open(image_path, 'rb') as f:
                upload_response = self.client.com.atproto.repo.upload_blob(f)
                return upload_response.blob
        except Exception as e:
            error_msg = f"Failed to upload image: {str(e)}"
            logger.error(error_msg)
            raise BlueskyPostError(error_msg)

    def _create_post_with_link(self, text: str, link_text: str, link_url: str, image_path: Optional[str] = None, alt_text: Optional[str] = None) -> None:
        """Create a post with a link and optional image.
        
        Args:
            text: The full text of the post
            link_text: The text that should be a clickable link
            link_url: The URL the link should point to
            image_path: Optional path to an image to include
            alt_text: Optional alt text for the image
        """
        try:
            # Find byte indices for the link text
            byte_text = text.encode('utf-8')
            text_pos = text.find(link_text)
            
            if text_pos == -1:
                raise ValueError(f"Could not find '{link_text}' in text: {text}")
                
            # Calculate byte positions
            byte_start = len(text[:text_pos].encode('utf-8'))
            byte_end = byte_start + len(link_text.encode('utf-8'))
            
            # Create facet for the link
            facets = [{
                'index': {
                    'byteStart': byte_start,
                    'byteEnd': byte_end
                },
                'features': [{
                    '$type': 'app.bsky.richtext.facet#link',
                    'uri': link_url
                }]
            }]

            # Create the post record
            record = {
                'text': text,
                'facets': facets,
                'createdAt': datetime.now(timezone.utc).isoformat(),
                '$type': 'app.bsky.feed.post'
            }

            # Add image if provided
            if image_path:
                self._validate_image(image_path)
                image_blob = self._upload_image(image_path)
                record['embed'] = {
                    "$type": "app.bsky.embed.images",
                    "images": [{"alt": alt_text or "", "image": image_blob}]
                }

            # Create the post with proper data structure
            data = {
                'collection': 'app.bsky.feed.post',
                'repo': self.client.me.did,
                'record': record
            }

            # Send the post
            self.client.com.atproto.repo.create_record(data=data)
            logger.info("Successfully posted to Bluesky")

        except Exception as e:
            logger.error(f"Failed to post to Bluesky: {str(e)}")
            raise BlueskyPostError(str(e))

    def post_thread(self, posts: List[Dict[str, str]]) -> None:
        """Post a thread of multiple posts to Bluesky.
        
        Args:
            posts: List of dictionaries containing post content and optional image paths.
                  Each dict should have 'text' key and optional 'image' and 'alt' keys.
                  For posts with links, include 'link_text' and 'link_url'.
                  
        Raises:
            BlueskyPostError: If thread posting fails.
        """
        try:
            logger.info(f"Starting thread post with {len(posts)} posts")
            
            if not posts:
                logger.warning("Empty posts list provided")
                return
                
            if os.getenv('TEST_MODE', 'false').lower() == 'true':
                # In test mode, just log what would be posted
                logger.info("TEST MODE: Simulating thread post")
                for i, post in enumerate(posts):
                    logger.info(f"\nPost {i+1}:")
                    logger.info(f"Text:\n{post['text']}")
                    if 'image' in post:
                        logger.info(f"Image: {post['image']}")
                        if 'alt' in post:
                            logger.info(f"Alt text: {post['alt']}")
                return

            # Post the first post and get its reference
            first_post = posts[0]
            if 'link_text' in first_post and 'link_url' in first_post:
                record = self._create_post_with_link(
                    first_post['text'],
                    first_post['link_text'],
                    first_post['link_url'],
                    first_post.get('image'),
                    first_post.get('alt', '')
                )
            else:
                if 'image' in first_post:
                    self._validate_image(first_post['image'])
                    image_blob = self._upload_image(first_post['image'])
                    record = self.client.send_post(
                        text=first_post['text'],
                        embed={"$type": "app.bsky.embed.images", "images": [{"alt": first_post.get('alt', ''), "image": image_blob}]}
                    )
                else:
                    record = self.client.send_post(text=first_post['text'])
                
            root_ref = {
                "uri": record.uri,
                "cid": record.cid
            }
            parent_ref = root_ref
            
            # Post the rest of the thread as replies
            for post in posts[1:]:
                # Add small delay to prevent rate limiting
                time.sleep(1)
                
                # Create reply with both root and parent references
                reply_ref = {
                    "root": root_ref,
                    "parent": parent_ref
                }
                
                if 'link_text' in post and 'link_url' in post:
                    record = self._create_post_with_link(
                        post['text'],
                        post['link_text'],
                        post['link_url'],
                        post.get('image'),
                        post.get('alt', ''),
                        reply_ref
                    )
                else:
                    if 'image' in post:
                        self._validate_image(post['image'])
                        image_blob = self._upload_image(post['image'])
                        record = self.client.send_post(
                            text=post['text'],
                            embed={"$type": "app.bsky.embed.images", "images": [{"alt": post.get('alt', ''), "image": image_blob}]},
                            reply_to=reply_ref
                        )
                    else:
                        record = self.client.send_post(
                            text=post['text'],
                            reply_to=reply_ref
                        )
                
                # Update parent reference for next post
                parent_ref = {
                    "uri": record.uri,
                    "cid": record.cid
                }
                
            logger.info("Thread posted successfully")
            
        except Exception as e:
            error_msg = f"Failed to post thread: {str(e)}"
            logger.error(error_msg)
            raise BlueskyPostError(error_msg)

    def post_daily_thread(self, day_comparison: Dict, leaderboard: Dict, permit_stats: Dict, heatmap_path: str, emergency_heatmap_path: str) -> None:
        """Post a daily summary thread with statistics and visualizations.
        
        Args:
            day_comparison: Dictionary containing day-of-week comparison metrics
            leaderboard: Dictionary containing contractor leaderboards
            permit_stats: Dictionary containing permit statistics
            heatmap_path: Path to the heatmap visualization
            emergency_heatmap_path: Path to the emergency heatmap visualization
            
        Raises:
            BlueskyPostError: If thread posting fails
        """
        try:
            logger.info("Preparing daily summary thread")
            
            # Format the main post with permit statistics
            main_post = f"🕳️ Chicago Hole Report - {day_comparison['day_name']}\n\n"
            
            main_post += f"Total Permits Starting to Dig: {permit_stats['total_count']:.0f}\n"
            main_post += f"     {'⬇️' if day_comparison['total_diff_percent'] < 0 else '⬆️'} {abs(day_comparison['total_diff_percent']):.0f}% vs {day_comparison['day_name']} avg\n\n"
            
            main_post += f"🔴 Emergency Permits: {permit_stats['emergency_count']:.0f}\n"
            main_post += f"     {'⬇️' if day_comparison['emergency_diff_percent'] < 0 else '⬆️'} {abs(day_comparison['emergency_diff_percent']):.0f}% vs {day_comparison['day_name']} avg\n\n"
            
            main_post += f"🔵 Regular Permits: {permit_stats['regular_count']:.0f}\n"
            main_post += f"     {'⬇️' if day_comparison['regular_diff_percent'] < 0 else '⬆️'} {abs(day_comparison['regular_diff_percent']):.0f}% vs {day_comparison['day_name']} avg"
            
            # Format the leaderboard post
            leaderboard_post = "🏆 Top Diggers\n\n"
            
            # Add overall leaders with medal emojis
            medals = ["🥇", "🥈", "🥉"]
            for i, leader in enumerate(leaderboard['overall'][:5]):
                if leader and 'name' in leader and 'count' in leader:
                    emoji = medals[i] if i < 3 else "👷"
                    leaderboard_post += f"{emoji} {leader['name']}: {leader['count']}\n"
            
            # Create thread with posts and images
            thread = [
                {
                    'text': main_post,
                    'image': emergency_heatmap_path,
                    'alt': "Heatmap showing distribution of dig permits in Chicago"
                },
                {
                    'text': leaderboard_post
                }
            ]
            
            # Post the thread
            self.post_thread(thread)
            logger.info("Daily summary thread posted successfully")
            
        except Exception as e:
            error_msg = f"Failed to post daily thread: {str(e)}"
            logger.error(error_msg)
            raise BlueskyPostError(error_msg)
