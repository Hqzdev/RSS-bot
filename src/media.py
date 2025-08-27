"""
Media processing and image handling
"""
import asyncio
import aiohttp
import aiofiles
import os
import hashlib
from typing import Optional, Tuple, BinaryIO
from PIL import Image, ImageDraw, ImageFont
import io
import logging
from urllib.parse import urlparse

from .config import settings
from .security import security_manager

logger = logging.getLogger(__name__)


class MediaProcessor:
    """Handles media processing and image manipulation"""
    
    def __init__(self):
        self.session = None
        self.cache_dir = "data/media_cache"
        self._ensure_cache_dir()
    
    def _ensure_cache_dir(self):
        """Ensure media cache directory exists"""
        os.makedirs(self.cache_dir, exist_ok=True)
    
    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def download_image(self, url: str) -> Optional[bytes]:
        """Download image from URL"""
        if not url:
            return None
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"Failed to download image {url}: HTTP {response.status}")
                    return None
                
                content_type = response.headers.get('content-type', '')
                if not content_type.startswith('image/'):
                    logger.warning(f"Invalid content type for image {url}: {content_type}")
                    return None
                
                return await response.read()
        
        except Exception as e:
            logger.error(f"Error downloading image {url}: {e}")
            return None
    
    async def process_image_for_post(self, image_data: bytes, max_size: int = None) -> Optional[bytes]:
        """Process image for Telegram post"""
        if not image_data:
            return None
        
        try:
            max_size = max_size or settings.max_image_size
            
            # Open image
            image = Image.open(io.BytesIO(image_data))
            
            # Convert to RGB if necessary
            if image.mode in ('RGBA', 'LA', 'P'):
                # Create white background
                background = Image.new('RGB', image.size, (255, 255, 255))
                if image.mode == 'P':
                    image = image.convert('RGBA')
                background.paste(image, mask=image.split()[-1] if image.mode == 'RGBA' else None)
                image = background
            
            # Resize if too large
            if max(image.size) > max_size:
                image.thumbnail((max_size, max_size), Image.Resampling.LANCZOS)
            
            # Save as JPEG
            output = io.BytesIO()
            image.save(output, format='JPEG', quality=85, optimize=True)
            return output.getvalue()
        
        except Exception as e:
            logger.error(f"Error processing image for post: {e}")
            return None
    
    async def process_image_for_story(self, image_data: bytes) -> Optional[bytes]:
        """Process image for Telegram story (9:16 aspect ratio)"""
        if not image_data:
            return None
        
        try:
            # Open image
            image = Image.open(io.BytesIO(image_data))
            
            # Convert to RGB if necessary
            if image.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', image.size, (255, 255, 255))
                if image.mode == 'P':
                    image = image.convert('RGBA')
                background.paste(image, mask=image.split()[-1] if image.mode == 'RGBA' else None)
                image = background
            
            # Calculate target dimensions (9:16 aspect ratio)
            target_width = settings.story_image_width
            target_height = settings.story_image_height
            
            # Calculate crop dimensions to maintain aspect ratio
            img_ratio = image.width / image.height
            target_ratio = target_width / target_height
            
            if img_ratio > target_ratio:
                # Image is wider than target, crop width
                new_width = int(image.height * target_ratio)
                left = (image.width - new_width) // 2
                image = image.crop((left, 0, left + new_width, image.height))
            else:
                # Image is taller than target, crop height
                new_height = int(image.width / target_ratio)
                top = (image.height - new_height) // 2
                image = image.crop((0, top, image.width, top + new_height))
            
            # Resize to target dimensions
            image = image.resize((target_width, target_height), Image.Resampling.LANCZOS)
            
            # Save as JPEG
            output = io.BytesIO()
            image.save(output, format='JPEG', quality=90, optimize=True)
            return output.getvalue()
        
        except Exception as e:
            logger.error(f"Error processing image for story: {e}")
            return None
    
    async def create_story_with_text(self, image_data: bytes, text: str, 
                                   font_size: int = 40, text_color: str = 'white') -> Optional[bytes]:
        """Create story image with text overlay"""
        if not image_data:
            return None
        
        try:
            # Process base image for story
            story_image_data = await self.process_image_for_story(image_data)
            if not story_image_data:
                return None
            
            # Open processed image
            image = Image.open(io.BytesIO(story_image_data))
            
            # Create drawing object
            draw = ImageDraw.Draw(image)
            
            # Try to load a font, fallback to default
            try:
                # Try to use a system font
                font = ImageFont.truetype("/System/Library/Fonts/Arial.ttf", font_size)
            except:
                try:
                    font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", font_size)
                except:
                    font = ImageFont.load_default()
            
            # Calculate text position (centered)
            bbox = draw.textbbox((0, 0), text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            
            x = (image.width - text_width) // 2
            y = image.height - text_height - 50  # 50px from bottom
            
            # Add text shadow for better readability
            shadow_offset = 2
            draw.text((x + shadow_offset, y + shadow_offset), text, 
                     fill='black', font=font)
            draw.text((x, y), text, fill=text_color, font=font)
            
            # Save result
            output = io.BytesIO()
            image.save(output, format='JPEG', quality=90, optimize=True)
            return output.getvalue()
        
        except Exception as e:
            logger.error(f"Error creating story with text: {e}")
            return None
    
    async def get_image_info(self, image_data: bytes) -> Optional[dict]:
        """Get image information"""
        if not image_data:
            return None
        
        try:
            image = Image.open(io.BytesIO(image_data))
            return {
                'format': image.format,
                'mode': image.mode,
                'size': image.size,
                'width': image.width,
                'height': image.height
            }
        except Exception as e:
            logger.error(f"Error getting image info: {e}")
            return None
    
    async def cache_image(self, url: str, image_data: bytes) -> Optional[str]:
        """Cache image to disk"""
        if not image_data:
            return None
        
        try:
            # Generate cache filename
            url_hash = hashlib.md5(url.encode()).hexdigest()
            extension = self._get_extension_from_url(url)
            filename = f"{url_hash}{extension}"
            filepath = os.path.join(self.cache_dir, filename)
            
            # Save to disk
            async with aiofiles.open(filepath, 'wb') as f:
                await f.write(image_data)
            
            return filepath
        
        except Exception as e:
            logger.error(f"Error caching image: {e}")
            return None
    
    def _get_extension_from_url(self, url: str) -> str:
        """Get file extension from URL"""
        parsed = urlparse(url)
        path = parsed.path.lower()
        
        if path.endswith('.jpg') or path.endswith('.jpeg'):
            return '.jpg'
        elif path.endswith('.png'):
            return '.png'
        elif path.endswith('.gif'):
            return '.gif'
        elif path.endswith('.webp'):
            return '.webp'
        else:
            return '.jpg'  # Default to JPEG
    
    async def cleanup_cache(self, max_age_hours: int = 24):
        """Clean up old cached images"""
        try:
            import time
            current_time = time.time()
            max_age_seconds = max_age_hours * 3600
            
            for filename in os.listdir(self.cache_dir):
                filepath = os.path.join(self.cache_dir, filename)
                if os.path.isfile(filepath):
                    file_age = current_time - os.path.getmtime(filepath)
                    if file_age > max_age_seconds:
                        os.remove(filepath)
                        logger.info(f"Removed old cached image: {filename}")
        
        except Exception as e:
            logger.error(f"Error cleaning up cache: {e}")
    
    async def extract_og_image(self, html_content: str, base_url: str) -> Optional[str]:
        """Extract og:image from HTML content"""
        try:
            from bs4 import BeautifulSoup
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Look for og:image meta tag
            og_image = soup.find('meta', property='og:image')
            if og_image and og_image.get('content'):
                image_url = og_image['content']
                if not image_url.startswith('http'):
                    # Make relative URL absolute
                    from urllib.parse import urljoin
                    image_url = urljoin(base_url, image_url)
                return image_url
            
            # Look for twitter:image meta tag
            twitter_image = soup.find('meta', attrs={'name': 'twitter:image'})
            if twitter_image and twitter_image.get('content'):
                image_url = twitter_image['content']
                if not image_url.startswith('http'):
                    from urllib.parse import urljoin
                    image_url = urljoin(base_url, image_url)
                return image_url
            
            # Look for first img tag
            img = soup.find('img')
            if img and img.get('src'):
                image_url = img['src']
                if not image_url.startswith('http'):
                    from urllib.parse import urljoin
                    image_url = urljoin(base_url, image_url)
                return image_url
            
            return None
        
        except Exception as e:
            logger.error(f"Error extracting og:image: {e}")
            return None
    
    async def validate_image(self, image_data: bytes) -> bool:
        """Validate image data"""
        if not image_data:
            return False
        
        try:
            image = Image.open(io.BytesIO(image_data))
            image.verify()  # Verify image integrity
            return True
        except Exception:
            return False
    
    async def get_image_size(self, image_data: bytes) -> Optional[Tuple[int, int]]:
        """Get image dimensions"""
        if not image_data:
            return None
        
        try:
            image = Image.open(io.BytesIO(image_data))
            return image.size
        except Exception as e:
            logger.error(f"Error getting image size: {e}")
            return None
