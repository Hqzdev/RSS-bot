"""
Database models and connection management
"""
import os
from datetime import datetime
from typing import Optional, List
from sqlalchemy import (
    create_engine, Column, Integer, String, Text, DateTime, 
    Boolean, ForeignKey, Index, Float
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import StaticPool

from .config import settings

Base = declarative_base()


class Feed(Base):
    """RSS feed source"""
    __tablename__ = "feeds"
    
    id = Column(Integer, primary_key=True)
    url = Column(String(500), nullable=False, unique=True)
    label = Column(String(100), nullable=True)
    lang = Column(String(10), default="ru")
    last_ok_at = Column(DateTime, nullable=True)
    last_error_at = Column(DateTime, nullable=True)
    last_error_msg = Column(Text, nullable=True)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    items = relationship("Item", back_populates="feed", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_feeds_url', 'url'),
        Index('idx_feeds_enabled', 'enabled'),
    )


class Item(Base):
    """RSS item/article"""
    __tablename__ = "items"
    
    id = Column(Integer, primary_key=True)
    feed_id = Column(Integer, ForeignKey("feeds.id"), nullable=False)
    guid = Column(String(500), nullable=False)
    title = Column(String(500), nullable=False)
    link = Column(String(1000), nullable=False)
    published_at = Column(DateTime, nullable=True)
    content_hash = Column(String(64), nullable=False)  # SHA-256
    has_media = Column(Boolean, default=False)
    summary = Column(Text, nullable=True)
    content = Column(Text, nullable=True)
    image_url = Column(String(1000), nullable=True)
    tags = Column(Text, nullable=True)  # JSON array
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    feed = relationship("Feed", back_populates="items")
    queue_items = relationship("QueueItem", back_populates="item", cascade="all, delete-orphan")
    publishes = relationship("Publish", back_populates="item", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_items_guid', 'guid'),
        Index('idx_items_content_hash', 'content_hash'),
        Index('idx_items_published_at', 'published_at'),
        Index('idx_items_feed_id', 'feed_id'),
    )


class QueueItem(Base):
    """Publication queue"""
    __tablename__ = "queue"
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)
    type = Column(String(20), nullable=False)  # post, story
    channel_id = Column(String(100), nullable=True)
    scheduled_at = Column(DateTime, nullable=True)
    status = Column(String(20), default="pending")  # pending, processing, completed, failed
    attempts = Column(Integer, default=0)
    last_attempt_at = Column(DateTime, nullable=True)
    error_msg = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    item = relationship("Item", back_populates="queue_items")
    
    __table_args__ = (
        Index('idx_queue_status', 'status'),
        Index('idx_queue_scheduled_at', 'scheduled_at'),
        Index('idx_queue_type', 'type'),
    )


class Publish(Base):
    """Published content tracking"""
    __tablename__ = "publishes"
    
    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey("items.id"), nullable=False)
    target = Column(String(100), nullable=False)  # channel_id or user_id
    type = Column(String(20), nullable=False)  # post, story
    message_id = Column(String(100), nullable=True)
    posted_at = Column(DateTime, default=datetime.utcnow)
    result = Column(Text, nullable=True)  # JSON with details
    views = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    item = relationship("Item", back_populates="publishes")
    
    __table_args__ = (
        Index('idx_publishes_target', 'target'),
        Index('idx_publishes_type', 'type'),
        Index('idx_publishes_posted_at', 'posted_at'),
    )


class Admin(Base):
    """Bot administrators"""
    __tablename__ = "admins"
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False, unique=True)
    role = Column(String(20), default="admin")  # admin, moderator
    username = Column(String(100), nullable=True)
    first_name = Column(String(100), nullable=True)
    last_name = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_admins_user_id', 'user_id'),
    )


class Template(Base):
    """Message templates"""
    __tablename__ = "templates"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    type = Column(String(20), nullable=False)  # post, story
    text = Column(Text, nullable=False)
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_templates_name', 'name'),
        Index('idx_templates_type', 'type'),
    )


class Setting(Base):
    """Application settings"""
    __tablename__ = "settings"
    
    id = Column(Integer, primary_key=True)
    key = Column(String(100), nullable=False, unique=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_settings_key', 'key'),
    )


class Blacklist(Base):
    """Blacklisted domains/keywords"""
    __tablename__ = "blacklist"
    
    id = Column(Integer, primary_key=True)
    pattern = Column(String(500), nullable=False)
    type = Column(String(20), nullable=False)  # domain, keyword
    reason = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_blacklist_pattern', 'pattern'),
        Index('idx_blacklist_type', 'type'),
    )


class Session(Base):
    """Encrypted session storage"""
    __tablename__ = "sessions"
    
    id = Column(Integer, primary_key=True)
    kind = Column(String(20), nullable=False)  # bot, user
    enc_blob = Column(Text, nullable=False)  # Encrypted session data
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_sessions_kind', 'kind'),
    )


# Database connection
def get_database_url():
    """Get database URL with proper configuration"""
    if settings.db_url.startswith("sqlite"):
        # Ensure data directory exists
        os.makedirs("data", exist_ok=True)
        return settings.db_url
    
    return settings.db_url


def create_engine_and_session():
    """Create database engine and session factory"""
    database_url = get_database_url()
    
    if database_url.startswith("sqlite"):
        engine = create_engine(
            database_url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            echo=False
        )
    else:
        engine = create_engine(database_url, echo=False)
    
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, SessionLocal


# Create tables
def create_tables(engine):
    """Create all database tables"""
    Base.metadata.create_all(bind=engine)


# Database dependency
def get_db():
    """Database session dependency"""
    engine, SessionLocal = create_engine_and_session()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
