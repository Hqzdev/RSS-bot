"""
Content normalization and enrichment
"""
import re
import json
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from bs4 import BeautifulSoup
import markdown
import logging

from .config import settings
from .security import security_manager

logger = logging.getLogger(__name__)


class ContentNormalizer:
    """Normalizes and enriches RSS content"""
    
    def __init__(self):
        self.md_converter = markdown.Markdown(
            extensions=['extra', 'codehilite', 'tables'],
            output_format='html5'
        )
    
    def normalize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a feed item"""
        try:
            # Clean and normalize text fields
            item['title'] = self._normalize_title(item.get('title', ''))
            item['summary'] = self._normalize_summary(item.get('summary', ''))
            item['content'] = self._normalize_content(item.get('content', ''))
            
            # Process links
            item['link'] = self._add_utm_parameters(item.get('link', ''))
            
            # Generate content hash for deduplication
            content_for_hash = f"{item['title']}{item['summary']}{item['content']}"
            item['content_hash'] = security_manager.hash_content(content_for_hash)
            
            # Generate hashtags
            item['hashtags'] = self._generate_hashtags(item)
            
            # Process image
            item['image_url'] = self._normalize_image_url(item.get('image_url'))
            
            # Detect language
            item['lang'] = self._detect_language(item.get('title', '') + ' ' + item.get('summary', ''))
            
            # Word count
            item['word_count'] = self._count_words(item.get('content', ''))
            
            return item
        
        except Exception as e:
            logger.error(f"Error normalizing item: {e}")
            return item
    
    def _normalize_title(self, title: str) -> str:
        """Normalize and clean title"""
        if not title:
            return ""
        
        # Remove HTML tags
        title = self._remove_html_tags(title)
        
        # Normalize whitespace
        title = re.sub(r'\s+', ' ', title).strip()
        
        # Limit length
        if len(title) > 200:
            title = title[:197] + "..."
        
        # Basic typography fixes
        title = self._fix_typography(title)
        
        return title
    
    def _normalize_summary(self, summary: str) -> str:
        """Normalize and clean summary"""
        if not summary:
            return ""
        
        # Remove HTML tags
        summary = self._remove_html_tags(summary)
        
        # Normalize whitespace
        summary = re.sub(r'\s+', ' ', summary).strip()
        
        # Limit to 2-3 sentences
        sentences = re.split(r'[.!?]+', summary)
        if len(sentences) > 3:
            summary = '. '.join(sentences[:3]) + '.'
        
        # Limit length
        if len(summary) > 300:
            summary = summary[:297] + "..."
        
        return summary
    
    def _normalize_content(self, content: str) -> str:
        """Normalize content and convert to Markdown"""
        if not content:
            return ""
        
        # Convert HTML to Markdown
        content = self._html_to_markdown(content)
        
        # Clean up markdown
        content = self._clean_markdown(content)
        
        # Limit length for posts
        if len(content) > 2000:
            content = content[:1997] + "..."
        
        return content
    
    def _html_to_markdown(self, html: str) -> str:
        """Convert HTML to Markdown"""
        if not html:
            return ""
        
        try:
            # Parse HTML
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Convert common HTML elements to Markdown
            markdown_text = ""
            
            for element in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'ul', 'ol', 'li', 'blockquote', 'pre', 'code', 'strong', 'em', 'a', 'img']):
                if element.name == 'p':
                    text = element.get_text().strip()
                    if text:
                        markdown_text += text + "\n\n"
                elif element.name.startswith('h'):
                    level = int(element.name[1])
                    text = element.get_text().strip()
                    if text:
                        markdown_text += '#' * level + ' ' + text + "\n\n"
                elif element.name == 'ul':
                    for li in element.find_all('li', recursive=False):
                        text = li.get_text().strip()
                        if text:
                            markdown_text += f"- {text}\n"
                    markdown_text += "\n"
                elif element.name == 'ol':
                    for i, li in enumerate(element.find_all('li', recursive=False), 1):
                        text = li.get_text().strip()
                        if text:
                            markdown_text += f"{i}. {text}\n"
                    markdown_text += "\n"
                elif element.name == 'blockquote':
                    text = element.get_text().strip()
                    if text:
                        markdown_text += f"> {text}\n\n"
                elif element.name == 'pre':
                    text = element.get_text().strip()
                    if text:
                        markdown_text += f"```\n{text}\n```\n\n"
                elif element.name == 'code':
                    text = element.get_text().strip()
                    if text:
                        markdown_text += f"`{text}`"
                elif element.name == 'strong':
                    text = element.get_text().strip()
                    if text:
                        markdown_text += f"**{text}**"
                elif element.name == 'em':
                    text = element.get_text().strip()
                    if text:
                        markdown_text += f"*{text}*"
                elif element.name == 'a':
                    text = element.get_text().strip()
                    href = element.get('href', '')
                    if text and href:
                        markdown_text += f"[{text}]({href})"
                elif element.name == 'img':
                    src = element.get('src', '')
                    alt = element.get('alt', '')
                    if src:
                        markdown_text += f"![{alt}]({src})\n\n"
            
            # If no structured content found, just get text
            if not markdown_text.strip():
                markdown_text = soup.get_text()
            
            return markdown_text.strip()
        
        except Exception as e:
            logger.error(f"Error converting HTML to Markdown: {e}")
            # Fallback: just remove HTML tags
            return self._remove_html_tags(html)
    
    def _clean_markdown(self, markdown_text: str) -> str:
        """Clean up markdown formatting"""
        if not markdown_text:
            return ""
        
        # Remove excessive newlines
        markdown_text = re.sub(r'\n{3,}', '\n\n', markdown_text)
        
        # Fix common markdown issues
        markdown_text = re.sub(r'([^\s])\*\*([^\s])', r'\1 **\2', markdown_text)  # Fix bold spacing
        markdown_text = re.sub(r'([^\s])\*([^\s])', r'\1 *\2', markdown_text)    # Fix italic spacing
        
        return markdown_text.strip()
    
    def _remove_html_tags(self, html: str) -> str:
        """Remove HTML tags from text"""
        if not html:
            return ""
        
        # Use BeautifulSoup to remove tags
        soup = BeautifulSoup(html, 'html.parser')
        return soup.get_text()
    
    def _fix_typography(self, text: str) -> str:
        """Fix common typography issues"""
        if not text:
            return ""
        
        # Fix quotes
        text = re.sub(r'"([^"]*)"', r'"\1"', text)
        text = re.sub(r"'([^']*)'", r"'\1'", text)
        
        # Fix dashes
        text = re.sub(r'--+', '—', text)
        
        # Fix spacing around punctuation
        text = re.sub(r'\s+([.,!?;:])', r'\1', text)
        
        return text
    
    def _add_utm_parameters(self, url: str) -> str:
        """Add UTM parameters to URL"""
        if not url or not settings.utm_on:
            return url
        
        try:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            
            # Add UTM parameters
            query['utm_source'] = [settings.utm_source]
            query['utm_medium'] = [settings.utm_medium]
            query['utm_campaign'] = [settings.utm_campaign]
            
            # Rebuild URL
            new_query = urlencode(query, doseq=True)
            new_parsed = parsed._replace(query=new_query)
            
            return urlunparse(new_parsed)
        
        except Exception as e:
            logger.error(f"Error adding UTM parameters: {e}")
            return url
    
    def _generate_hashtags(self, item: Dict[str, Any]) -> List[str]:
        """Generate hashtags for the item"""
        hashtags = []
        
        # Extract keywords from title and content
        text = f"{item.get('title', '')} {item.get('summary', '')} {item.get('content', '')}"
        text = text.lower()
        
        # Common hashtags based on content
        common_tags = {
            'новости': ['новости', 'новость', 'новостей'],
            'технологии': ['технологии', 'технология', 'tech', 'technology'],
            'ai': ['искусственный интеллект', 'ai', 'artificial intelligence', 'машинное обучение'],
            'telegram': ['telegram', 'телеграм'],
            'программирование': ['программирование', 'код', 'разработка', 'coding'],
            'криптовалюта': ['криптовалюта', 'биткоин', 'blockchain', 'crypto'],
            'игры': ['игры', 'game', 'gaming'],
            'финансы': ['финансы', 'экономика', 'деньги', 'finance'],
            'спорт': ['спорт', 'футбол', 'баскетбол', 'sport'],
            'политика': ['политика', 'политик', 'государство'],
            'наука': ['наука', 'исследование', 'science'],
            'здоровье': ['здоровье', 'медицина', 'health'],
            'образование': ['образование', 'учеба', 'education'],
            'культура': ['культура', 'искусство', 'art'],
            'путешествия': ['путешествия', 'туризм', 'travel']
        }
        
        # Check for hashtag patterns
        for tag, keywords in common_tags.items():
            for keyword in keywords:
                if keyword in text:
                    hashtags.append(f"#{tag}")
                    break
        
        # Extract domain-based hashtags
        domain = self._extract_domain(item.get('link', ''))
        if domain:
            domain_tag = domain.replace('.', '_').replace('-', '_')
            hashtags.append(f"#{domain_tag}")
        
        # Add language tag
        lang = item.get('lang', 'ru')
        if lang == 'en':
            hashtags.append('#english')
        elif lang == 'ru':
            hashtags.append('#русский')
        
        # Limit to 5 hashtags
        return hashtags[:5]
    
    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL"""
        if not url:
            return None
        
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Remove www prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            
            return domain
        except Exception:
            return None
    
    def _normalize_image_url(self, image_url: Optional[str]) -> Optional[str]:
        """Normalize image URL"""
        if not image_url:
            return None
        
        try:
            # Ensure absolute URL
            if not image_url.startswith('http'):
                return None
            
            # Remove tracking parameters
            parsed = urlparse(image_url)
            query = parse_qs(parsed.query)
            
            # Remove common tracking parameters
            tracking_params = ['utm_', 'fbclid', 'gclid', 'ref', 'source']
            for param in tracking_params:
                query = {k: v for k, v in query.items() if not k.startswith(param)}
            
            # Rebuild URL
            new_query = urlencode(query, doseq=True)
            new_parsed = parsed._replace(query=new_query)
            
            return urlunparse(new_parsed)
        
        except Exception as e:
            logger.error(f"Error normalizing image URL: {e}")
            return image_url
    
    def _detect_language(self, text: str) -> str:
        """Simple language detection"""
        if not text:
            return 'ru'
        
        # Count Cyrillic vs Latin characters
        cyrillic_count = len(re.findall(r'[а-яё]', text.lower()))
        latin_count = len(re.findall(r'[a-z]', text.lower()))
        
        if cyrillic_count > latin_count:
            return 'ru'
        elif latin_count > cyrillic_count:
            return 'en'
        else:
            return 'ru'  # Default to Russian
    
    def _count_words(self, text: str) -> int:
        """Count words in text"""
        if not text:
            return 0
        
        # Remove HTML tags and count words
        clean_text = self._remove_html_tags(text)
        words = re.findall(r'\b\w+\b', clean_text)
        return len(words)
    
    def create_post_template(self, item: Dict[str, Any], template_name: str = "default") -> str:
        """Create post text from template"""
        if template_name == "default":
            template = """{title}

{summary}

Источник: {source_domain}
{short_url}
{hashtags}"""
        else:
            # Load custom template from database (implement later)
            template = """{title}

{summary}

Источник: {source_domain}
{short_url}
{hashtags}"""
        
        # Prepare template variables
        source_domain = self._extract_domain(item.get('link', '')) or 'unknown'
        hashtags = ' '.join(item.get('hashtags', []))
        short_url = item.get('link', '')  # Will be shortened later
        
        return template.format(
            title=item.get('title', ''),
            summary=item.get('summary', ''),
            source_domain=source_domain,
            short_url=short_url,
            hashtags=hashtags
        )
    
    def create_story_template(self, item: Dict[str, Any]) -> str:
        """Create story text"""
        title = item.get('title', '')
        summary = item.get('summary', '')
        
        # Limit for story format
        if len(title) > 50:
            title = title[:47] + "..."
        
        if len(summary) > 100:
            summary = summary[:97] + "..."
        
        return f"{title}\n\n{summary}"
