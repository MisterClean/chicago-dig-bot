"""Module for posting updates to Bluesky social network."""
from atproto import Client
from atproto.exceptions import AtProtocolError
import pandas as pd
from typing import Dict, List, Optional
from pathlib import Path
import time
import os
from utils.logging import get_logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = get_logger(__name__)

class BlueskyPostError(Exception):
    """Custom exception for Bluesky posting errors."""
    pass

class BlueskyPoster:
    """Handles posting updates to Bluesky social network."""
    
    def __init__(self):
        """Initialize Bluesky client with authentication."""
        self.client = None
        
        if not os.getenv('TEST_MODE', 'false').lower() == 'true':
            try:
                logger.info("Initializing Bluesky client")
                
                # Get credentials directly from environment
                handle = os.getenv('BLUESKY_HANDLE')
                password = os.getenv('BLUESKY_PASSWORD')
                
                if not handle or not password:
                    raise BlueskyPostError("Missing Bluesky credentials in environment")
                
                logger.info(f"Attempting to login with handle: {handle}")
                
                self.client = Client()
                self.client.login(handle, password)
                logger.info("Successfully authenticated with Bluesky")
                
            except AtProtocolError as e:
                error_msg = f"Bluesky authentication failed: {str(e)}"
                logger.error(error_msg)
                raise BlueskyPostError(error_msg)
            except Exception as e:
                error_msg = f"Failed to initialize Bluesky client: {str(e)}"
                logger.error(error_msg)
                raise BlueskyPostError(error_msg)
        else:
            logger.info("Running in test mode - Bluesky client disabled")
            
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

    def post_thread(self, posts: List[Dict[str, str]]) -> None:
        """Post a thread of multiple posts to Bluesky.
        
        Args:
            posts: List of dictionaries containing post content and optional image paths.
                  Each dict should have 'text' key and optional 'image' and 'alt' keys.
                  
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
            if 'image' in first_post:
                self._validate_image(first_post['image'])
                image_blob = self._upload_image(first_post['image'])
                root_post = self.client.send_post(
                    text=first_post['text'],
                    embed={"$type": "app.bsky.embed.images", "images": [{"alt": first_post.get('alt', ''), "image": image_blob}]}
                )
            else:
                root_post = self.client.send_post(text=first_post['text'])
                
            root_ref = {
                "uri": root_post.uri,
                "cid": root_post.cid
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
                
                if 'image' in post:
                    self._validate_image(post['image'])
                    image_blob = self._upload_image(post['image'])
                    parent_post = self.client.send_post(
                        text=post['text'],
                        embed={"$type": "app.bsky.embed.images", "images": [{"alt": post.get('alt', ''), "image": image_blob}]},
                        reply_to=reply_ref
                    )
                else:
                    parent_post = self.client.send_post(
                        text=post['text'],
                        reply_to=reply_ref
                    )
                
                # Update parent reference for next post
                parent_ref = {
                    "uri": parent_post.uri,
                    "cid": parent_post.cid
                }
                
            logger.info("Thread posted successfully")
            
        except Exception as e:
            error_msg = f"Failed to post thread: {str(e)}"
            logger.error(error_msg)
            raise BlueskyPostError(error_msg)

    def _make_post(self, text: str, image_path: Optional[str] = None, alt_text: Optional[str] = None) -> None:
        """Helper method to post content with test mode support.
        
        Args:
            text: Text content to post.
            image_path: Optional path to image file.
            alt_text: Optional alt text for image.
            
        Raises:
            BlueskyPostError: If posting fails.
        """
        if os.getenv('TEST_MODE', 'false').lower() == 'true':
            logger.info("TEST MODE: Simulating Bluesky post")
            logger.info(f"Text:\n{text}")
            if image_path:
                logger.info(f"Image: {image_path}")
                if alt_text:
                    logger.info(f"Alt text: {alt_text}")
        else:
            try:
                logger.info("Posting to Bluesky")
                if image_path:
                    logger.debug(f"Posting with image: {image_path}")
                    image_blob = self._upload_image(image_path)
                    self.client.send_post(
                        text=text,
                        embed={"$type": "app.bsky.embed.images", "images": [{"alt": alt_text or "", "image": image_blob}]}
                    )
                else:
                    logger.debug("Posting text only")
                    self.client.send_post(text=text)
                    
                logger.info("Successfully posted to Bluesky")
                
            except AtProtocolError as e:
                error_msg = f"Bluesky API error: {str(e)}"
                logger.error(error_msg)
                raise BlueskyPostError(error_msg)
            except Exception as e:
                error_msg = f"Failed to post to Bluesky: {str(e)}"
                logger.error(error_msg)
                raise BlueskyPostError(error_msg)
