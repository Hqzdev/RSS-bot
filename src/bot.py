"""
Main Telegram bot module with admin commands
"""
import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
from telegram.error import TelegramError

from .config import settings
from .database import get_db, Feed, Item, Admin, Template, Setting, Blacklist, Session
from .ingest import RSSIngester, FeedItem
from .normalizer import ContentNormalizer
from .publisher import TelegramPublisher
from .security import security_manager

logger = logging.getLogger(__name__)


class RSSBot:
    """Main RSS bot class"""
    
    def __init__(self):
        self.application = None
        self.publisher = TelegramPublisher()
        self.normalizer = ContentNormalizer()
        self.is_running = False
    
    async def initialize(self):
        """Initialize bot and components"""
        try:
            await self.publisher.initialize()
            self.application = Application.builder().token(settings.telegram_bot_token).build()
            self._add_handlers()
            await self._initialize_database()
            logger.info("RSS Bot initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing bot: {e}")
            raise
    
    def _add_handlers(self):
        """Add command and message handlers"""
        self.application.add_handler(CommandHandler("start", self._cmd_start))
        self.application.add_handler(CommandHandler("help", self._cmd_help))
        self.application.add_handler(CommandHandler("status", self._cmd_status))
        self.application.add_handler(CommandHandler("addfeed", self._cmd_addfeed))
        self.application.add_handler(CommandHandler("feeds", self._cmd_feeds))
        self.application.add_handler(CommandHandler("delfeed", self._cmd_delfeed))
        self.application.add_handler(CommandHandler("setchannel", self._cmd_setchannel))
        self.application.add_handler(CommandHandler("moderation", self._cmd_moderation))
        self.application.add_handler(CommandHandler("login_user", self._cmd_login_user))
        self.application.add_handler(CallbackQueryHandler(self._handle_callback_query))
        self.application.add_error_handler(self._error_handler)
    
    async def _initialize_database(self):
        """Initialize database with default data"""
        try:
            db = next(get_db())
            
            # Add default templates
            default_post_template = db.query(Template).filter(
                Template.name == "default", Template.type == "post"
            ).first()
            
            if not default_post_template:
                default_post_template = Template(
                    name="default", type="post",
                    text="""{title}

{summary}

Источник: {source_domain}
{short_url}
{hashtags}""",
                    is_default=True
                )
                db.add(default_post_template)
            
            # Add default settings
            default_settings = [
                ("moderation_enabled", "true"),
                ("auto_posting", "false"),
                ("default_channel", ""),
                ("poll_interval_minutes", str(settings.base_poll_minutes))
            ]
            
            for key, value in default_settings:
                setting = db.query(Setting).filter(Setting.key == key).first()
                if not setting:
                    setting = Setting(key=key, value=value)
                    db.add(setting)
            
            db.commit()
            logger.info("Database initialized with default data")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
    
    async def start(self):
        """Start the bot"""
        if self.is_running:
            return
        
        try:
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            self.is_running = True
            logger.info("RSS Bot started successfully")
        except Exception as e:
            logger.error(f"Error starting bot: {e}")
            raise
    
    async def stop(self):
        """Stop the bot"""
        if not self.is_running:
            return
        
        try:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
            await self.publisher.close()
            self.is_running = False
            logger.info("RSS Bot stopped")
        except Exception as e:
            logger.error(f"Error stopping bot: {e}")
    
    async def _check_admin(self, update: Update) -> bool:
        """Check if user is admin"""
        user_id = update.effective_user.id
        return user_id in settings.admin_ids
    
    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user_id = update.effective_user.id
        
        if await self._check_admin(update):
            welcome_text = """🤖 *RSS Bot* - Агрегатор новостей

Вы авторизованы как администратор.

*Основные команды:*
• `/addfeed <url>` - добавить RSS источник
• `/feeds` - список источников
• `/status` - статус системы
• `/help` - подробная справка

*Настройки:*
• `/moderation on/off` - включить/выключить модерацию
• `/setchannel <@channel>` - установить канал по умолчанию

*Истории:*
• `/login_user` - авторизация для историй

Используйте `/help` для полного списка команд."""
        else:
            welcome_text = """🤖 *RSS Bot* - Агрегатор новостей

У вас нет прав администратора.
Обратитесь к администратору бота."""
        
        await update.message.reply_text(welcome_text, parse_mode='Markdown')
    
    async def _cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        if not await self._check_admin(update):
            await update.message.reply_text("Недостаточно прав.")
            return
        
        help_text = """📚 *Справка по командам RSS Bot*

*Управление источниками:*
• `/addfeed <url> [label] [lang]` - добавить RSS источник
• `/delfeed <id|url>` - удалить источник
• `/feeds` - список всех источников

*Публикация:*
• `/setchannel <@channel|chat_id>` - установить канал по умолчанию
• `/moderation <on|off>` - включить/выключить модерацию

*Истории (MTProto):*
• `/login_user` - авторизация для публикации историй

*Мониторинг:*
• `/status` - статус системы и метрики

*Примеры:*
• `/addfeed https://example.com/rss "Новости" ru`
• `/setchannel @mychannel`
• `/moderation on`"""
        
        await update.message.reply_text(help_text, parse_mode='Markdown')
    
    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /status command"""
        if not await self._check_admin(update):
            await update.message.reply_text("Недостаточно прав.")
            return
        
        try:
            db = next(get_db())
            
            total_feeds = db.query(Feed).count()
            enabled_feeds = db.query(Feed).filter(Feed.enabled == True).count()
            total_items = db.query(Item).count()
            
            yesterday = datetime.utcnow() - timedelta(days=1)
            recent_items = db.query(Item).filter(Item.created_at >= yesterday).count()
            
            status_text = f"""📊 *Статус RSS Bot*

*Источники:*
• Всего: {total_feeds}
• Активных: {enabled_feeds}

*Контент:*
• Всего статей: {total_items}
• Новых (24ч): {recent_items}

*Система:*
• Модерация: {'Включена' if await self._get_setting('moderation_enabled') == 'true' else 'Выключена'}
• MTProto: {'Авторизован' if self.publisher.is_user_authorized else 'Не авторизован'}"""
            
            await update.message.reply_text(status_text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            await update.message.reply_text(f"Ошибка получения статуса: {e}")
    
    async def _cmd_addfeed(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /addfeed command"""
        if not await self._check_admin(update):
            await update.message.reply_text("Недостаточно прав.")
            return
        
        if not context.args:
            await update.message.reply_text("Использование: `/addfeed <url> [label] [lang]`", parse_mode='Markdown')
            return
        
        url = context.args[0]
        label = context.args[1] if len(context.args) > 1 else None
        lang = context.args[2] if len(context.args) > 2 else "ru"
        
        try:
            if not url.startswith(('http://', 'https://')):
                await update.message.reply_text("❌ Неверный URL. Должен начинаться с http:// или https://")
                return
            
            db = next(get_db())
            
            existing_feed = db.query(Feed).filter(Feed.url == url).first()
            if existing_feed:
                await update.message.reply_text("❌ Этот источник уже добавлен.")
                return
            
            await update.message.reply_text("🔄 Тестирование источника...")
            
            async with RSSIngester() as ingester:
                success, items, error = await ingester.fetch_feed(url)
                
                if not success:
                    await update.message.reply_text(f"❌ Ошибка при тестировании: {error}")
                    return
                
                feed = Feed(
                    url=url, label=label, lang=lang,
                    last_ok_at=datetime.utcnow(), enabled=True
                )
                db.add(feed)
                db.commit()
                
                await update.message.reply_text(
                    f"✅ Источник добавлен успешно!\n\n"
                    f"URL: {url}\n"
                    f"Метка: {label or 'Не указана'}\n"
                    f"Язык: {lang}\n"
                    f"Найдено статей: {len(items)}"
                )
        except Exception as e:
            logger.error(f"Error adding feed: {e}")
            await update.message.reply_text(f"❌ Ошибка при добавлении источника: {e}")
    
    async def _cmd_feeds(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /feeds command"""
        if not await self._check_admin(update):
            await update.message.reply_text("Недостаточно прав.")
            return
        
        try:
            db = next(get_db())
            feeds = db.query(Feed).order_by(Feed.created_at.desc()).all()
            
            if not feeds:
                await update.message.reply_text("📭 Нет добавленных источников.")
                return
            
            text = "📰 *Список источников:*\n\n"
            
            for feed in feeds:
                status = "✅" if feed.enabled else "❌"
                label = feed.label or "Без метки"
                item_count = db.query(Item).filter(Item.feed_id == feed.id).count()
                
                text += f"{status} *{label}*\n"
                text += f"URL: `{feed.url}`\n"
                text += f"Язык: {feed.lang}\n"
                text += f"Статей: {item_count}\n"
                text += f"ID: {feed.id}\n\n"
            
            if len(text) > 4096:
                parts = [text[i:i+4096] for i in range(0, len(text), 4096)]
                for part in parts:
                    await update.message.reply_text(part, parse_mode='Markdown')
            else:
                await update.message.reply_text(text, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error listing feeds: {e}")
            await update.message.reply_text(f"❌ Ошибка при получении списка: {e}")
    
    async def _cmd_delfeed(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /delfeed command"""
        if not await self._check_admin(update):
            await update.message.reply_text("Недостаточно прав.")
            return
        
        if not context.args:
            await update.message.reply_text("Использование: `/delfeed <id|url>`", parse_mode='Markdown')
            return
        
        identifier = context.args[0]
        
        try:
            db = next(get_db())
            
            if identifier.isdigit():
                feed = db.query(Feed).filter(Feed.id == int(identifier)).first()
            else:
                feed = db.query(Feed).filter(Feed.url == identifier).first()
            
            if not feed:
                await update.message.reply_text("❌ Источник не найден.")
                return
            
            db.delete(feed)
            db.commit()
            
            await update.message.reply_text(f"✅ Источник '{feed.label or feed.url}' удален.")
        except Exception as e:
            logger.error(f"Error deleting feed: {e}")
            await update.message.reply_text(f"❌ Ошибка при удалении: {e}")
    
    async def _cmd_setchannel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /setchannel command"""
        if not await self._check_admin(update):
            await update.message.reply_text("Недостаточно прав.")
            return
        
        if not context.args:
            await update.message.reply_text("Использование: `/setchannel <@channel|chat_id>`", parse_mode='Markdown')
            return
        
        channel = context.args[0]
        
        try:
            chat = await self.publisher.bot.get_chat(channel)
            if chat.type not in ['channel', 'group', 'supergroup']:
                await update.message.reply_text("❌ Указанный чат не является каналом или группой.")
                return
            
            await self._set_setting('default_channel', channel)
            await update.message.reply_text(f"✅ Канал по умолчанию установлен: {channel}")
        except TelegramError:
            await update.message.reply_text("❌ Не удается получить доступ к каналу. Проверьте права бота.")
        except Exception as e:
            logger.error(f"Error setting channel: {e}")
            await update.message.reply_text(f"❌ Ошибка при установке канала: {e}")
    
    async def _cmd_moderation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /moderation command"""
        if not await self._check_admin(update):
            await update.message.reply_text("Недостаточно прав.")
            return
        
        if not context.args:
            current = await self._get_setting('moderation_enabled')
            status = "включена" if current == 'true' else "выключена"
            await update.message.reply_text(f"Модерация: {status}\n\nИспользование: `/moderation <on|off>`", parse_mode='Markdown')
            return
        
        mode = context.args[0].lower()
        
        if mode not in ['on', 'off']:
            await update.message.reply_text("Использование: `/moderation <on|off>`", parse_mode='Markdown')
            return
        
        try:
            await self._set_setting('moderation_enabled', 'true' if mode == 'on' else 'false')
            status = "включена" if mode == 'on' else "выключена"
            await update.message.reply_text(f"✅ Модерация {status}.")
        except Exception as e:
            logger.error(f"Error setting moderation: {e}")
            await update.message.reply_text(f"❌ Ошибка при изменении настроек: {e}")
    
    async def _cmd_login_user(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /login_user command for MTProto authorization"""
        if not await self._check_admin(update):
            await update.message.reply_text("Недостаточно прав.")
            return
        
        if not all([settings.api_id, settings.api_hash]):
            await update.message.reply_text("❌ MTProto credentials не настроены в конфигурации.")
            return
        
        await update.message.reply_text(
            "📱 *Авторизация для историй*\n\n"
            "Для публикации историй нужна авторизация через MTProto.\n\n"
            "1. Получите API_ID и API_HASH на https://my.telegram.org\n"
            "2. Добавьте их в конфигурацию\n"
            "3. Перезапустите бота\n\n"
            "После этого используйте команду `/login_user` для авторизации.",
            parse_mode='Markdown'
        )
    
    async def _handle_callback_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle callback queries from inline keyboards"""
        try:
            await self.publisher.handle_callback_query(update.callback_query)
        except Exception as e:
            logger.error(f"Error handling callback query: {e}")
            await update.callback_query.answer("Ошибка обработки")
    
    async def _error_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle errors"""
        logger.error(f"Exception while handling an update: {context.error}")
    
    async def _get_setting(self, key: str) -> str:
        """Get setting value from database"""
        try:
            db = next(get_db())
            setting = db.query(Setting).filter(Setting.key == key).first()
            return setting.value if setting else None
        except Exception as e:
            logger.error(f"Error getting setting {key}: {e}")
            return None
    
    async def _set_setting(self, key: str, value: str):
        """Set setting value in database"""
        try:
            db = next(get_db())
            setting = db.query(Setting).filter(Setting.key == key).first()
            
            if setting:
                setting.value = value
                setting.updated_at = datetime.utcnow()
            else:
                setting = Setting(key=key, value=value)
                db.add(setting)
            
            db.commit()
        except Exception as e:
            logger.error(f"Error setting {key}: {e}")
            raise
