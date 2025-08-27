"""
RSS/Atom/JSON-feed ingestion and HTML fallback parsing
"""
import asyncio
import aiohttp
import feedparser
import json
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from readability import Document
import logging

from .config import settings

logger = logging.getLogger(__name__)


class FeedItem:
    """Normalized feed item"""
    
    def __init__(self, **kwargs):
        self.guid = kwargs.get('guid', '')
        self.title = kwargs.get('title', '')
        self.link = kwargs.get('link', '')
        self.published_at = kwargs.get('published_at')
        self.summary = kwargs.get('summary', '')
        self.content = kwargs.get('content', '')
        self.image_url = kwargs.get('image_url')
        self.tags = kwargs.get('tags', [])
        self.author = kwargs.get('author', '')
        self.feed_url = kwargs.get('feed_url', '')
        self.feed_title = kwargs.get('feed_title', '')
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'guid': self.guid,
            'title': self.title,
            'link': self.link,
            'published_at': self.published_at,
            'summary': self.summary,
            'content': self.content,
            'image_url': self.image_url,
            'tags': self.tags,
            'author': self.author,
            'feed_url': self.feed_url,
            'feed_title': self.feed_title
        }


class RSSIngester:
    """RSS/Atom/JSON-feed ingester"""
    
    def __init__(self):
        self.session = None
        self.timeout = aiohttp.ClientTimeout(total=30)
    
    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout,
            headers={
                'User-Agent': 'RSS-Bot/1.0 (Telegram RSS Aggregator)'
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def fetch_feed(self, url: str) -> Tuple[bool, List[FeedItem], str]:
        """
        Fetch and parse feed from URL
        
        Returns:
            Tuple of (success, items, error_message)
        """
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return False, [], f"HTTP {response.status}: {response.reason}"
                
                content = await response.text()
                content_type = response.headers.get('content-type', '').lower()
                
                # Try different parsers based on content type
                if 'json' in content_type or url.endswith('.json'):
                    return await self._parse_json_feed(content, url)
                elif 'xml' in content_type or 'rss' in content_type or 'atom' in content_type:
                    return await self._parse_xml_feed(content, url)
                else:
                    # Try XML first, then JSON, then HTML fallback
                    success, items, error = await self._parse_xml_feed(content, url)
                    if success:
                        return success, items, error
                    
                    success, items, error = await self._parse_json_feed(content, url)
                    if success:
                        return success, items, error
                    
                    return await self._parse_html_fallback(content, url)
        
        except asyncio.TimeoutError:
            return False, [], "Request timeout"
        except Exception as e:
            logger.error(f"Error fetching feed {url}: {e}")
            return False, [], str(e)
    
    async def _parse_xml_feed(self, content: str, url: str) -> Tuple[bool, List[FeedItem], str]:
        """Parse RSS/Atom XML feed"""
        try:
            feed = feedparser.parse(content)
            
            if feed.bozo:
                return False, [], f"Feed parsing error: {feed.bozo_exception}"
            
            items = []
            feed_title = getattr(feed.feed, 'title', '')
            
            for entry in feed.entries:
                item = FeedItem(
                    guid=self._extract_guid(entry),
                    title=self._clean_text(getattr(entry, 'title', '')),
                    link=self._extract_link(entry, url),
                    published_at=self._parse_date(entry),
                    summary=self._clean_text(getattr(entry, 'summary', '')),
                    content=self._extract_content(entry),
                    image_url=self._extract_image(entry),
                    tags=self._extract_tags(entry),
                    author=getattr(entry, 'author', ''),
                    feed_url=url,
                    feed_title=feed_title
                )
                items.append(item)
            
            return True, items, ""
        
        except Exception as e:
            logger.error(f"Error parsing XML feed: {e}")
            return False, [], str(e)
    
    async def _parse_json_feed(self, content: str, url: str) -> Tuple[bool, List[FeedItem], str]:
        """Parse JSON Feed (RFC 4287)"""
        try:
            data = json.loads(content)
            
            if not isinstance(data, dict) or 'version' not in data:
                return False, [], "Invalid JSON Feed format"
            
            items = []
            feed_title = data.get('title', '')
            
            for entry in data.get('items', []):
                item = FeedItem(
                    guid=entry.get('id', ''),
                    title=self._clean_text(entry.get('title', '')),
                    link=entry.get('url', ''),
                    published_at=self._parse_json_date(entry.get('date_published')),
                    summary=self._clean_text(entry.get('summary', '')),
                    content=entry.get('content_text', ''),
                    image_url=self._extract_json_image(entry),
                    tags=entry.get('tags', []),
                    author=entry.get('authors', [{}])[0].get('name', '') if entry.get('authors') else '',
                    feed_url=url,
                    feed_title=feed_title
                )
                items.append(item)
            
            return True, items, ""
        
        except json.JSONDecodeError as e:
            return False, [], f"Invalid JSON: {e}"
        except Exception as e:
            logger.error(f"Error parsing JSON feed: {e}")
            return False, [], str(e)
    
    async def _parse_html_fallback(self, content: str, url: str) -> Tuple[bool, List[FeedItem], str]:
        """HTML fallback using Readability-like parsing"""
        try:
            # Use readability to extract main content
            doc = Document(content)
            title = doc.title()
            content_text = doc.summary()
            
            # Try to find more articles in the page
            soup = BeautifulSoup(content, 'html.parser')
            articles = []
            
            # Look for common article selectors
            selectors = [
                'article', '.article', '.post', '.entry',
                '[class*="article"]', '[class*="post"]', '[class*="entry"]'
            ]
            
            for selector in selectors:
                articles.extend(soup.select(selector))
                if articles:
                    break
            
            items = []
            
            if articles:
                # Parse multiple articles
                for article in articles[:10]:  # Limit to 10 articles
                    item = FeedItem(
                        guid=self._generate_guid(article, url),
                        title=self._clean_text(article.get_text()[:100]),
                        link=url,
                        published_at=datetime.now(timezone.utc),
                        summary=self._clean_text(article.get_text()[:200]),
                        content=self._clean_text(article.get_text()),
                        image_url=self._extract_html_image(article),
                        tags=[],
                        author='',
                        feed_url=url,
                        feed_title=title or urlparse(url).netloc
                    )
                    items.append(item)
            else:
                # Single article fallback
                item = FeedItem(
                    guid=self._generate_guid(soup, url),
                    title=title or urlparse(url).netloc,
                    link=url,
                    published_at=datetime.now(timezone.utc),
                    summary=self._clean_text(content_text[:200]),
                    content=self._clean_text(content_text),
                    image_url=self._extract_html_image(soup),
                    tags=[],
                    author='',
                    feed_url=url,
                    feed_title=title or urlparse(url).netloc
                )
                items.append(item)
            
            return True, items, ""
        
        except Exception as e:
            logger.error(f"Error parsing HTML fallback: {e}")
            return False, [], str(e)
    
    def _extract_guid(self, entry) -> str:
        """Extract GUID from feed entry"""
        # Try different GUID fields
        for field in ['id', 'guid', 'link']:
            if hasattr(entry, field):
                value = getattr(entry, field)
                if value:
                    return str(value)
        
        # Fallback: hash of title + link
        title = getattr(entry, 'title', '')
        link = self._extract_link(entry, '')
        return f"{hash(title + link)}"
    
    def _extract_link(self, entry, base_url: str) -> str:
        """Extract link from feed entry"""
        if hasattr(entry, 'link'):
            link = entry.link
            if link and not link.startswith('http'):
                link = urljoin(base_url, link)
            return link
        return ''
    
    def _parse_date(self, entry) -> Optional[datetime]:
        """Parse publication date"""
        date_fields = ['published_parsed', 'updated_parsed', 'created_parsed']
        
        for field in date_fields:
            if hasattr(entry, field):
                parsed = getattr(entry, field)
                if parsed:
                    try:
                        return datetime(*parsed[:6], tzinfo=timezone.utc)
                    except (ValueError, TypeError):
                        continue
        
        return None
    
    def _parse_json_date(self, date_str: str) -> Optional[datetime]:
        """Parse JSON Feed date"""
        if not date_str:
            return None
        
        try:
            # Try ISO format
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError:
            try:
                # Try RFC 3339 format
                return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')
            except ValueError:
                return None
    
    def _extract_content(self, entry) -> str:
        """Extract content from feed entry"""
        # Try different content fields
        content_fields = ['content', 'summary', 'description']
        
        for field in content_fields:
            if hasattr(entry, field):
                content = getattr(entry, field)
                if content:
                    if isinstance(content, list) and len(content) > 0:
                        return content[0].get('value', '')
                    elif isinstance(content, str):
                        return content
        
        return ''
    
    def _extract_image(self, entry) -> Optional[str]:
        """Extract image URL from feed entry"""
        # Check media content
        if hasattr(entry, 'media_content') and entry.media_content:
            for media in entry.media_content:
                if media.get('type', '').startswith('image/'):
                    return media.get('url')
        
        # Check enclosures
        if hasattr(entry, 'enclosures') and entry.enclosures:
            for enclosure in entry.enclosures:
                if enclosure.get('type', '').startswith('image/'):
                    return enclosure.get('href')
        
        # Check links
        if hasattr(entry, 'links'):
            for link in entry.links:
                if link.get('type', '').startswith('image/'):
                    return link.get('href')
        
        return None
    
    def _extract_json_image(self, entry) -> Optional[str]:
        """Extract image from JSON Feed entry"""
        # Check image field
        if 'image' in entry:
            return entry['image']
        
        # Check attachments
        for attachment in entry.get('attachments', []):
            if attachment.get('mime_type', '').startswith('image/'):
                return attachment.get('url')
        
        return None
    
    def _extract_html_image(self, element) -> Optional[str]:
        """Extract image from HTML element"""
        # Look for og:image meta tag
        og_image = element.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            return og_image['content']
        
        # Look for first img tag
        img = element.find('img')
        if img and img.get('src'):
            return img['src']
        
        return None
    
    def _extract_tags(self, entry) -> List[str]:
        """Extract tags from feed entry"""
        tags = []
        
        # Check tags field
        if hasattr(entry, 'tags'):
            for tag in entry.tags:
                if hasattr(tag, 'term'):
                    tags.append(tag.term)
        
        # Check category field
        if hasattr(entry, 'category'):
            tags.append(entry.category)
        
        return tags
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if not text:
            return ''
        
        # Remove HTML tags
        soup = BeautifulSoup(text, 'html.parser')
        text = soup.get_text()
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def _generate_guid(self, element, url: str) -> str:
        """Generate GUID for HTML element"""
        import hashlib
        content = element.get_text()[:100] + url
        return hashlib.sha256(content.encode()).hexdigest()
