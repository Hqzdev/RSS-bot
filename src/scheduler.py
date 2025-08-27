"""
Scheduler for RSS feed polling and content publishing
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import json

from .config import settings
from .database import get_db, Feed, Item, QueueItem, Publish, Setting
from .ingest import RSSIngester, FeedItem
from .normalizer import ContentNormalizer
from .publisher import TelegramPublisher
from .security import security_manager

logger = logging.getLogger(__name__)


class RSSScheduler:
    """Scheduler for RSS feed processing"""
    
    def __init__(self, publisher: TelegramPublisher):
        self.publisher = publisher
        self.scheduler = AsyncIOScheduler()
        self.normalizer = ContentNormalizer()
        self.is_running = False
    
    async def start(self):
        """Start the scheduler"""
        if self.is_running:
            return
        
        try:
            # Add jobs
            self._add_feed_polling_job()
            self._add_queue_processing_job()
            self._add_digest_job()
            self._add_cleanup_job()
            
            # Start scheduler
            self.scheduler.start()
            self.is_running = True
            
            logger.info("RSS Scheduler started successfully")
        
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            raise
    
    def stop(self):
        """Stop the scheduler"""
        if not self.is_running:
            return
        
        try:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("RSS Scheduler stopped")
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
    
    def _add_feed_polling_job(self):
        """Add feed polling job"""
        # Poll feeds every N minutes with jitter
        interval_minutes = settings.base_poll_minutes
        
        self.scheduler.add_job(
            self._poll_feeds,
            IntervalTrigger(minutes=interval_minutes, jitter=interval_minutes * 0.3),
            id='feed_polling',
            name='Poll RSS feeds',
            replace_existing=True
        )
        
        logger.info(f"Added feed polling job (every {interval_minutes} minutes)")
    
    def _add_queue_processing_job(self):
        """Add queue processing job"""
        # Process queue every minute
        self.scheduler.add_job(
            self._process_queue,
            IntervalTrigger(minutes=1),
            id='queue_processing',
            name='Process publication queue',
            replace_existing=True
        )
        
        logger.info("Added queue processing job (every minute)")
    
    def _add_digest_job(self):
        """Add digest job"""
        try:
            # Parse cron expression
            cron_parts = settings.digest_cron.split()
            if len(cron_parts) == 5:
                minute, hour, day, month, day_of_week = cron_parts
                
                self.scheduler.add_job(
                    self._create_digest,
                    CronTrigger(
                        minute=minute,
                        hour=hour,
                        day=day,
                        month=month,
                        day_of_week=day_of_week
                    ),
                    id='digest_creation',
                    name='Create and publish digest',
                    replace_existing=True
                )
                
                logger.info(f"Added digest job (cron: {settings.digest_cron})")
            else:
                logger.warning(f"Invalid digest cron format: {settings.digest_cron}")
        
        except Exception as e:
            logger.error(f"Error adding digest job: {e}")
    
    def _add_cleanup_job(self):
        """Add cleanup job"""
        # Cleanup every 6 hours
        self.scheduler.add_job(
            self._cleanup_old_data,
            IntervalTrigger(hours=6),
            id='cleanup',
            name='Cleanup old data',
            replace_existing=True
        )
        
        logger.info("Added cleanup job (every 6 hours)")
    
    async def _poll_feeds(self):
        """Poll all enabled RSS feeds"""
        try:
            db = next(get_db())
            feeds = db.query(Feed).filter(Feed.enabled == True).all()
            
            if not feeds:
                logger.info("No enabled feeds to poll")
                return
            
            logger.info(f"Polling {len(feeds)} feeds...")
            
            async with RSSIngester() as ingester:
                for feed in feeds:
                    try:
                        await self._poll_single_feed(ingester, feed, db)
                    except Exception as e:
                        logger.error(f"Error polling feed {feed.url}: {e}")
                        await self._mark_feed_error(feed, str(e), db)
            
            logger.info("Feed polling completed")
        
        except Exception as e:
            logger.error(f"Error in feed polling: {e}")
    
    async def _poll_single_feed(self, ingester: RSSIngester, feed: Feed, db):
        """Poll a single RSS feed"""
        try:
            # Fetch feed
            success, items, error = await ingester.fetch_feed(feed.url)
            
            if not success:
                await self._mark_feed_error(feed, error, db)
                return
            
            # Process new items
            new_items_count = 0
            for feed_item in items:
                try:
                    # Check if item already exists
                    existing_item = db.query(Item).filter(
                        Item.feed_id == feed.id,
                        Item.guid == feed_item.guid
                    ).first()
                    
                    if existing_item:
                        continue
                    
                    # Normalize item
                    item_dict = feed_item.to_dict()
                    item_dict['feed_id'] = feed.id
                    normalized_item = self.normalizer.normalize_item(item_dict)
                    
                    # Save to database
                    db_item = Item(
                        feed_id=feed.id,
                        guid=normalized_item['guid'],
                        title=normalized_item['title'],
                        link=normalized_item['link'],
                        published_at=normalized_item.get('published_at'),
                        content_hash=normalized_item['content_hash'],
                        has_media=bool(normalized_item.get('image_url')),
                        summary=normalized_item.get('summary'),
                        content=normalized_item.get('content'),
                        image_url=normalized_item.get('image_url'),
                        tags=json.dumps(normalized_item.get('hashtags', []))
                    )
                    
                    db.add(db_item)
                    db.flush()  # Get the ID
                    
                    # Check if moderation is enabled
                    moderation_enabled = await self._get_setting('moderation_enabled')
                    
                    if moderation_enabled == 'true':
                        # Send to moderation
                        await self._send_to_moderation(db_item, db)
                    else:
                        # Auto-publish
                        await self._add_to_queue(db_item, 'post', db)
                    
                    new_items_count += 1
                
                except Exception as e:
                    logger.error(f"Error processing item from {feed.url}: {e}")
                    continue
            
            # Mark feed as successful
            feed.last_ok_at = datetime.utcnow()
            feed.last_error_at = None
            feed.last_error_msg = None
            db.commit()
            
            if new_items_count > 0:
                logger.info(f"Added {new_items_count} new items from {feed.url}")
        
        except Exception as e:
            logger.error(f"Error polling feed {feed.url}: {e}")
            await self._mark_feed_error(feed, str(e), db)
    
    async def _mark_feed_error(self, feed: Feed, error_msg: str, db):
        """Mark feed as having an error"""
        try:
            feed.last_error_at = datetime.utcnow()
            feed.last_error_msg = error_msg
            db.commit()
            
            logger.error(f"Feed {feed.url} error: {error_msg}")
        except Exception as e:
            logger.error(f"Error marking feed error: {e}")
    
    async def _send_to_moderation(self, item: Item, db):
        """Send item to moderation"""
        try:
            # Get admin IDs
            admin_ids = settings.admin_ids
            
            if not admin_ids:
                logger.warning("No admin IDs configured for moderation")
                return
            
            # Prepare item data
            item_dict = {
                'id': item.id,
                'title': item.title,
                'summary': item.summary,
                'content': item.content,
                'link': item.link,
                'image_url': item.image_url,
                'hashtags': json.loads(item.tags) if item.tags else [],
                'word_count': len(item.content.split()) if item.content else 0,
                'lang': 'ru',  # Default
                'feed_id': item.feed_id
            }
            
            # Send moderation preview
            results = await self.publisher.send_moderation_preview(item_dict, admin_ids)
            
            logger.info(f"Sent item {item.id} to moderation for {len(results)} admins")
        
        except Exception as e:
            logger.error(f"Error sending to moderation: {e}")
    
    async def _add_to_queue(self, item: Item, pub_type: str, db, scheduled_at: datetime = None):
        """Add item to publication queue"""
        try:
            queue_item = QueueItem(
                item_id=item.id,
                type=pub_type,
                scheduled_at=scheduled_at,
                status="pending"
            )
            
            db.add(queue_item)
            db.commit()
            
            logger.info(f"Added item {item.id} to queue for {pub_type}")
        
        except Exception as e:
            logger.error(f"Error adding to queue: {e}")
    
    async def _process_queue(self):
        """Process publication queue"""
        try:
            db = next(get_db())
            
            # Get pending items
            pending_items = db.query(QueueItem).filter(
                QueueItem.status == "pending",
                (QueueItem.scheduled_at.is_(None) | (QueueItem.scheduled_at <= datetime.utcnow()))
            ).limit(10).all()  # Process max 10 items at once
            
            if not pending_items:
                return
            
            logger.info(f"Processing {len(pending_items)} queue items")
            
            for queue_item in pending_items:
                try:
                    await self._process_queue_item(queue_item, db)
                except Exception as e:
                    logger.error(f"Error processing queue item {queue_item.id}: {e}")
                    queue_item.status = "failed"
                    queue_item.error_msg = str(e)
                    queue_item.attempts += 1
                    queue_item.last_attempt_at = datetime.utcnow()
                    db.commit()
        
        except Exception as e:
            logger.error(f"Error processing queue: {e}")
    
    async def _process_queue_item(self, queue_item: QueueItem, db):
        """Process a single queue item"""
        try:
            # Get item
            item = db.query(Item).filter(Item.id == queue_item.item_id).first()
            if not item:
                logger.error(f"Item {queue_item.item_id} not found")
                queue_item.status = "failed"
                queue_item.error_msg = "Item not found"
                db.commit()
                return
            
            # Mark as processing
            queue_item.status = "processing"
            queue_item.last_attempt_at = datetime.utcnow()
            db.commit()
            
            # Prepare item data
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
            
            # Get default channel
            default_channel = await self._get_setting('default_channel')
            
            if queue_item.type == "post":
                if not default_channel:
                    raise Exception("No default channel configured")
                
                success, error, message_id = await self.publisher.publish_post(
                    item_dict, default_channel
                )
                
                if success:
                    queue_item.status = "completed"
                    logger.info(f"Published post {item.id} to {default_channel}")
                else:
                    raise Exception(f"Failed to publish post: {error}")
            
            elif queue_item.type == "story":
                # For stories, we need to know which admin to send to
                # This would typically come from the moderation callback
                # For now, we'll skip stories without admin context
                queue_item.status = "failed"
                queue_item.error_msg = "Story publication requires admin context"
                logger.warning(f"Skipping story {item.id} - no admin context")
            
            db.commit()
        
        except Exception as e:
            logger.error(f"Error processing queue item {queue_item.id}: {e}")
            queue_item.status = "failed"
            queue_item.error_msg = str(e)
            queue_item.attempts += 1
            db.commit()
    
    async def _create_digest(self):
        """Create and publish digest"""
        try:
            db = next(get_db())
            
            # Get top items from last 24 hours
            yesterday = datetime.utcnow() - timedelta(days=1)
            
            top_items = db.query(Item).filter(
                Item.created_at >= yesterday
            ).order_by(Item.word_count.desc()).limit(10).all()
            
            if not top_items:
                logger.info("No items for digest")
                return
            
            # Create digest text
            digest_text = "📰 *Дайджест за последние 24 часа*\n\n"
            
            for i, item in enumerate(top_items, 1):
                digest_text += f"{i}. [{item.title}]({item.link})\n"
                if item.summary:
                    summary = item.summary[:100] + "..." if len(item.summary) > 100 else item.summary
                    digest_text += f"   {summary}\n"
                digest_text += "\n"
            
            # Get default channel
            default_channel = await self._get_setting('default_channel')
            if not default_channel:
                logger.warning("No default channel for digest")
                return
            
            # Publish digest
            success, error, message_id = await self.publisher.publish_post(
                {'title': 'Дайджест', 'summary': digest_text, 'link': '', 'hashtags': []},
                default_channel
            )
            
            if success:
                logger.info(f"Published digest with {len(top_items)} items")
            else:
                logger.error(f"Failed to publish digest: {error}")
        
        except Exception as e:
            logger.error(f"Error creating digest: {e}")
    
    async def _cleanup_old_data(self):
        """Clean up old data"""
        try:
            db = next(get_db())
            
            # Clean up old items (older than 30 days)
            thirty_days_ago = datetime.utcnow() - timedelta(days=30)
            old_items = db.query(Item).filter(Item.created_at < thirty_days_ago).all()
            
            for item in old_items:
                db.delete(item)
            
            # Clean up old queue items (older than 7 days)
            seven_days_ago = datetime.utcnow() - timedelta(days=7)
            old_queue_items = db.query(QueueItem).filter(
                QueueItem.created_at < seven_days_ago,
                QueueItem.status.in_(["completed", "failed"])
            ).all()
            
            for queue_item in old_queue_items:
                db.delete(queue_item)
            
            # Clean up old publications (older than 30 days)
            old_publishes = db.query(Publish).filter(Publish.posted_at < thirty_days_ago).all()
            
            for publish in old_publishes:
                db.delete(publish)
            
            db.commit()
            
            logger.info(f"Cleaned up {len(old_items)} old items, {len(old_queue_items)} queue items, {len(old_publishes)} publishes")
        
        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
    
    async def _get_setting(self, key: str) -> str:
        """Get setting value from database"""
        try:
            db = next(get_db())
            setting = db.query(Setting).filter(Setting.key == key).first()
            return setting.value if setting else None
        except Exception as e:
            logger.error(f"Error getting setting {key}: {e}")
            return None
