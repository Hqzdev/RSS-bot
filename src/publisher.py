"""
Telegram publishing module - Bot API for posts, MTProto for stories
"""
import asyncio
import json
import logging
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.error import TelegramError
from pyrogram import Client
from pyrogram.errors import FloodWait, SessionRevoked
import aioredis

from .config import settings
from .security import security_manager
from .database import get_db, Feed, Item, QueueItem, Publish, Admin, Template

logger = logging.getLogger(__name__)


class TelegramPublisher:
    """Handles Telegram publishing via Bot API and MTProto"""
    
    def __init__(self):
        self.bot = None
        self.user_client = None
        self.redis = None
        self.is_user_authorized = False
    
    async def initialize(self):
        """Initialize publishers"""
        # Initialize Bot API
        self.bot = Bot(token=settings.telegram_bot_token)
        
        # Initialize Redis
        self.redis = aioredis.from_url(settings.redis_url)
        
        # Initialize MTProto client for stories
        await self._initialize_mtproto()
    
    async def _initialize_mtproto(self):
        """Initialize MTProto client for user session"""
        if not all([settings.api_id, settings.api_hash]):
            logger.warning("MTProto credentials not configured. Stories will be disabled.")
            return
        
        try:
            # Try to load existing session
            session_data = await self._load_user_session()
            if session_data:
                self.user_client = Client(
                    "rss_bot_user",
                    api_id=settings.api_id,
                    api_hash=settings.api_hash,
                    session_string=session_data.get('session_string')
                )
                await self.user_client.start()
                self.is_user_authorized = True
                logger.info("MTProto user session loaded successfully")
            else:
                logger.info("No user session found. Use /login_user to authorize for stories.")
        
        except Exception as e:
            logger.error(f"Error initializing MTProto client: {e}")
    
    async def _load_user_session(self) -> Optional[Dict[str, Any]]:
        """Load encrypted user session from database"""
        try:
            db = next(get_db())
            session_record = db.query(Session).filter(Session.kind == "user").first()
            if session_record:
                return security_manager.decrypt_data(session_record.enc_blob)
        except Exception as e:
            logger.error(f"Error loading user session: {e}")
        return None
    
    async def _save_user_session(self, session_data: Dict[str, Any]):
        """Save encrypted user session to database"""
        try:
            encrypted_data = security_manager.encrypt_data(session_data)
            if encrypted_data:
                db = next(get_db())
                session_record = db.query(Session).filter(Session.kind == "user").first()
                if session_record:
                    session_record.enc_blob = encrypted_data
                    session_record.updated_at = datetime.utcnow()
                else:
                    session_record = Session(
                        kind="user",
                        enc_blob=encrypted_data
                    )
                    db.add(session_record)
                db.commit()
                logger.info("User session saved successfully")
        except Exception as e:
            logger.error(f"Error saving user session: {e}")
    
    async def publish_post(self, item: Dict[str, Any], channel_id: str, 
                          template_name: str = "default") -> Tuple[bool, str, Optional[str]]:
        """
        Publish post to Telegram channel via Bot API
        
        Returns:
            Tuple of (success, error_message, message_id)
        """
        try:
            # Get template
            post_text = await self._get_post_text(item, template_name)
            
            # Prepare inline keyboard
            keyboard = self._create_post_keyboard(item)
            
            # Check if we have an image
            image_url = item.get('image_url')
            image_data = None
            
            if image_url:
                from .media import MediaProcessor
                async with MediaProcessor() as media_processor:
                    image_data = await media_processor.download_image(image_url)
                    if image_data:
                        image_data = await media_processor.process_image_for_post(image_data)
            
            # Send message
            if image_data:
                # Send photo with caption
                message = await self.bot.send_photo(
                    chat_id=channel_id,
                    photo=image_data,
                    caption=post_text,
                    parse_mode='Markdown',
                    reply_markup=keyboard
                )
            else:
                # Send text message
                message = await self.bot.send_message(
                    chat_id=channel_id,
                    text=post_text,
                    parse_mode='Markdown',
                    reply_markup=keyboard
                )
            
            # Record publication
            await self._record_publication(item, channel_id, "post", str(message.message_id))
            
            logger.info(f"Post published successfully to {channel_id}")
            return True, "", str(message.message_id)
        
        except TelegramError as e:
            error_msg = f"Telegram API error: {e}"
            logger.error(error_msg)
            return False, error_msg, None
        except Exception as e:
            error_msg = f"Error publishing post: {e}"
            logger.error(error_msg)
            return False, error_msg, None
    
    async def publish_story(self, item: Dict[str, Any], user_id: str) -> Tuple[bool, str]:
        """
        Publish story via MTProto user session
        
        Returns:
            Tuple of (success, error_message)
        """
        if not self.is_user_authorized or not self.user_client:
            return False, "User session not authorized. Use /login_user first."
        
        try:
            # Get story text
            story_text = await self._get_story_text(item)
            
            # Get and process image
            image_url = item.get('image_url')
            if not image_url:
                return False, "No image available for story"
            
            from .media import MediaProcessor
            async with MediaProcessor() as media_processor:
                image_data = await media_processor.download_image(image_url)
                if not image_data:
                    return False, "Failed to download image"
                
                # Create story with text overlay
                story_image = await media_processor.create_story_with_text(image_data, story_text)
                if not story_image:
                    return False, "Failed to create story image"
                
                # Send story
                await self.user_client.send_photo(
                    chat_id=user_id,
                    photo=story_image,
                    caption=story_text
                )
            
            # Record publication
            await self._record_publication(item, user_id, "story")
            
            logger.info(f"Story published successfully to {user_id}")
            return True, ""
        
        except FloodWait as e:
            error_msg = f"Flood wait: {e.value} seconds"
            logger.warning(error_msg)
            return False, error_msg
        except SessionRevoked:
            error_msg = "User session revoked. Please re-authorize with /login_user"
            logger.error(error_msg)
            self.is_user_authorized = False
            return False, error_msg
        except Exception as e:
            error_msg = f"Error publishing story: {e}"
            logger.error(error_msg)
            return False, error_msg
    
    async def send_moderation_preview(self, item: Dict[str, Any], admin_ids: List[int]) -> List[Tuple[int, str]]:
        """Send moderation preview to admins"""
        results = []
        
        try:
            # Create preview text
            preview_text = self._create_preview_text(item)
            
            # Create moderation keyboard
            keyboard = self._create_moderation_keyboard(item)
            
            # Send to each admin
            for admin_id in admin_ids:
                try:
                    # Send preview with moderation buttons
                    message = await self.bot.send_message(
                        chat_id=admin_id,
                        text=preview_text,
                        parse_mode='Markdown',
                        reply_markup=keyboard
                    )
                    
                    # Store moderation data in Redis
                    moderation_key = f"moderation:{message.message_id}"
                    moderation_data = {
                        'item_id': item.get('id'),
                        'admin_id': admin_id,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                    await self.redis.setex(
                        moderation_key, 
                        3600,  # 1 hour TTL
                        json.dumps(moderation_data)
                    )
                    
                    results.append((admin_id, str(message.message_id)))
                
                except TelegramError as e:
                    logger.error(f"Error sending moderation preview to {admin_id}: {e}")
                    results.append((admin_id, f"Error: {e}"))
        
        except Exception as e:
            logger.error(f"Error creating moderation preview: {e}")
        
        return results
    
    async def _get_post_text(self, item: Dict[str, Any], template_name: str) -> str:
        """Get post text from template"""
        try:
            db = next(get_db())
            template = db.query(Template).filter(
                Template.name == template_name,
                Template.type == "post"
            ).first()
            
            if template:
                # Use custom template
                text = template.text
            else:
                # Use default template
                text = """{title}

{summary}

Источник: {source_domain}
{short_url}
{hashtags}"""
            
            # Prepare variables
            source_domain = self._extract_domain(item.get('link', '')) or 'unknown'
            hashtags = ' '.join(item.get('hashtags', []))
            short_url = item.get('link', '')  # Will be shortened later
            
            return text.format(
                title=item.get('title', ''),
                summary=item.get('summary', ''),
                source_domain=source_domain,
                short_url=short_url,
                hashtags=hashtags
            )
        
        except Exception as e:
            logger.error(f"Error getting post text: {e}")
            return f"{item.get('title', '')}\n\n{item.get('summary', '')}"
    
    async def _get_story_text(self, item: Dict[str, Any]) -> str:
        """Get story text"""
        try:
            db = next(get_db())
            template = db.query(Template).filter(
                Template.name == "default",
                Template.type == "story"
            ).first()
            
            if template:
                text = template.text
            else:
                # Default story template
                title = item.get('title', '')
                summary = item.get('summary', '')
                
                # Limit for story format
                if len(title) > 50:
                    title = title[:47] + "..."
                if len(summary) > 100:
                    summary = summary[:97] + "..."
                
                text = f"{title}\n\n{summary}"
            
            return text.format(
                title=item.get('title', ''),
                summary=item.get('summary', '')
            )
        
        except Exception as e:
            logger.error(f"Error getting story text: {e}")
            return item.get('title', '')[:100]
    
    def _create_post_keyboard(self, item: Dict[str, Any]) -> InlineKeyboardMarkup:
        """Create inline keyboard for post"""
        keyboard = []
        
        # Read button
        keyboard.append([
            InlineKeyboardButton("📖 Читать", url=item.get('link', ''))
        ])
        
        # Share button
        keyboard.append([
            InlineKeyboardButton("📤 Поделиться", switch_inline_query=item.get('title', ''))
        ])
        
        return InlineKeyboardMarkup(keyboard)
    
    def _create_moderation_keyboard(self, item: Dict[str, Any]) -> InlineKeyboardMarkup:
        """Create moderation keyboard"""
        keyboard = []
        
        # Main actions
        keyboard.append([
            InlineKeyboardButton("✅ Опубликовать", callback_data=f"publish_post:{item.get('id')}"),
            InlineKeyboardButton("📱 В историю", callback_data=f"publish_story:{item.get('id')}")
        ])
        
        # Delay options
        keyboard.append([
            InlineKeyboardButton("⏰ 30 мин", callback_data=f"delay:30:{item.get('id')}"),
            InlineKeyboardButton("⏰ 2 часа", callback_data=f"delay:120:{item.get('id')}")
        ])
        
        # Edit and ban
        keyboard.append([
            InlineKeyboardButton("✏️ Править", callback_data=f"edit:{item.get('id')}"),
            InlineKeyboardButton("🚫 Бан источник", callback_data=f"ban_source:{item.get('feed_id')}")
        ])
        
        return InlineKeyboardMarkup(keyboard)
    
    def _create_preview_text(self, item: Dict[str, Any]) -> str:
        """Create preview text for moderation"""
        text = f"*Новая статья для модерации*\n\n"
        text += f"*{item.get('title', '')}*\n\n"
        text += f"{item.get('summary', '')}\n\n"
        text += f"Источник: {self._extract_domain(item.get('link', ''))}\n"
        text += f"Слов: {item.get('word_count', 0)}\n"
        text += f"Язык: {item.get('lang', 'ru')}\n"
        
        if item.get('hashtags'):
            text += f"Теги: {' '.join(item.get('hashtags', []))}\n"
        
        return text
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        if not url:
            return 'unknown'
        
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            # Remove www prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            
            return domain
        except Exception:
            return 'unknown'
    
    async def _record_publication(self, item: Dict[str, Any], target: str, 
                                pub_type: str, message_id: str = None):
        """Record publication in database"""
        try:
            db = next(get_db())
            publication = Publish(
                item_id=item.get('id'),
                target=target,
                type=pub_type,
                message_id=message_id,
                posted_at=datetime.utcnow(),
                result=json.dumps({'success': True})
            )
            db.add(publication)
            db.commit()
        except Exception as e:
            logger.error(f"Error recording publication: {e}")
    
    async def handle_callback_query(self, callback_query) -> bool:
        """Handle moderation callback queries"""
        try:
            data = callback_query.data
            admin_id = callback_query.from_user.id
            
            # Check if user is admin
            if admin_id not in settings.admin_ids:
                await callback_query.answer("Недостаточно прав")
                return False
            
            if data.startswith("publish_post:"):
                item_id = int(data.split(":")[1])
                await self._handle_publish_post(callback_query, item_id)
            
            elif data.startswith("publish_story:"):
                item_id = int(data.split(":")[1])
                await self._handle_publish_story(callback_query, item_id)
            
            elif data.startswith("delay:"):
                parts = data.split(":")
                delay_minutes = int(parts[1])
                item_id = int(parts[2])
                await self._handle_delay_publication(callback_query, item_id, delay_minutes)
            
            elif data.startswith("edit:"):
                item_id = int(data.split(":")[1])
                await self._handle_edit_item(callback_query, item_id)
            
            elif data.startswith("ban_source:"):
                feed_id = int(data.split(":")[1])
                await self._handle_ban_source(callback_query, feed_id)
            
            return True
        
        except Exception as e:
            logger.error(f"Error handling callback query: {e}")
            await callback_query.answer("Ошибка обработки")
            return False
    
    async def _handle_publish_post(self, callback_query, item_id: int):
        """Handle post publication request"""
        try:
            # Get item from database
            db = next(get_db())
            item = db.query(Item).filter(Item.id == item_id).first()
            
            if not item:
                await callback_query.answer("Статья не найдена")
                return
            
            # Get default channel
            default_channel = await self._get_default_channel()
            if not default_channel:
                await callback_query.answer("Канал по умолчанию не настроен")
                return
            
            # Publish post
            item_dict = {
                'id': item.id,
                'title': item.title,
                'summary': item.summary,
                'content': item.content,
                'link': item.link,
                'image_url': item.image_url,
                'hashtags': json.loads(item.tags) if item.tags else [],
                'word_count': len(item.content.split()) if item.content else 0,
                'lang': 'ru'  # Default
            }
            
            success, error, message_id = await self.publish_post(item_dict, default_channel)
            
            if success:
                await callback_query.answer("Пост опубликован!")
                await callback_query.edit_message_text(
                    callback_query.message.text + "\n\n✅ Опубликован"
                )
            else:
                await callback_query.answer(f"Ошибка: {error}")
        
        except Exception as e:
            logger.error(f"Error handling post publication: {e}")
            await callback_query.answer("Ошибка публикации")
    
    async def _handle_publish_story(self, callback_query, item_id: int):
        """Handle story publication request"""
        try:
            # Get item from database
            db = next(get_db())
            item = db.query(Item).filter(Item.id == item_id).first()
            
            if not item:
                await callback_query.answer("Статья не найдена")
                return
            
            # Publish story to admin's account
            item_dict = {
                'id': item.id,
                'title': item.title,
                'summary': item.summary,
                'content': item.content,
                'link': item.link,
                'image_url': item.image_url,
                'hashtags': json.loads(item.tags) if item.tags else [],
                'word_count': len(item.content.split()) if item.content else 0,
                'lang': 'ru'  # Default
            }
            
            admin_id = str(callback_query.from_user.id)
            success, error = await self.publish_story(item_dict, admin_id)
            
            if success:
                await callback_query.answer("История опубликована!")
                await callback_query.edit_message_text(
                    callback_query.message.text + "\n\n📱 История опубликована"
                )
            else:
                await callback_query.answer(f"Ошибка: {error}")
        
        except Exception as e:
            logger.error(f"Error handling story publication: {e}")
            await callback_query.answer("Ошибка публикации")
    
    async def _handle_delay_publication(self, callback_query, item_id: int, delay_minutes: int):
        """Handle delayed publication"""
        try:
            # Add to queue with delay
            scheduled_time = datetime.utcnow() + timedelta(minutes=delay_minutes)
            
            db = next(get_db())
            queue_item = QueueItem(
                item_id=item_id,
                type="post",
                scheduled_at=scheduled_time,
                status="pending"
            )
            db.add(queue_item)
            db.commit()
            
            await callback_query.answer(f"Отложено на {delay_minutes} минут")
            await callback_query.edit_message_text(
                callback_query.message.text + f"\n\n⏰ Отложено на {delay_minutes} мин"
            )
        
        except Exception as e:
            logger.error(f"Error handling delay: {e}")
            await callback_query.answer("Ошибка отложенной публикации")
    
    async def _handle_edit_item(self, callback_query, item_id: int):
        """Handle item editing request"""
        await callback_query.answer("Редактирование пока не реализовано")
    
    async def _handle_ban_source(self, callback_query, feed_id: int):
        """Handle source banning"""
        try:
            db = next(get_db())
            feed = db.query(Feed).filter(Feed.id == feed_id).first()
            
            if feed:
                feed.enabled = False
                db.commit()
                await callback_query.answer("Источник заблокирован")
                await callback_query.edit_message_text(
                    callback_query.message.text + "\n\n🚫 Источник заблокирован"
                )
            else:
                await callback_query.answer("Источник не найден")
        
        except Exception as e:
            logger.error(f"Error banning source: {e}")
            await callback_query.answer("Ошибка блокировки")
    
    async def _get_default_channel(self) -> Optional[str]:
        """Get default channel from settings"""
        try:
            # This would typically come from database settings
            # For now, return None to indicate no default channel
            return None
        except Exception as e:
            logger.error(f"Error getting default channel: {e}")
            return None
    
    async def close(self):
        """Clean up resources"""
        if self.bot:
            await self.bot.close()
        
        if self.user_client:
            await self.user_client.stop()
        
        if self.redis:
            await self.redis.close()
