"""
Configuration management for RSS Bot
"""
import os
from typing import List, Optional
from pydantic import BaseSettings, Field
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """Application settings with validation"""
    
    # Telegram Bot Configuration
    telegram_bot_token: str = Field(..., env="TELEGRAM_BOT_TOKEN")
    admin_ids: List[int] = Field(default_factory=list, env="ADMIN_IDS")
    
    # Database Configuration
    db_url: str = Field(default="sqlite:///data/db.sqlite3", env="DB_URL")
    
    # Redis Configuration
    redis_url: str = Field(default="redis://localhost:6379/0", env="REDIS_URL")
    
    # Polling Configuration
    base_poll_minutes: int = Field(default=10, env="BASE_POLL_MINUTES")
    digest_cron: str = Field(default="0 9 * * *", env="DIGEST_CRON")
    
    # Content Filtering
    allow_langs: List[str] = Field(default=["ru", "en"], env="ALLOW_LANGS")
    min_words: int = Field(default=30, env="MIN_WORDS")
    dedup_window_days: int = Field(default=7, env="DEDUP_WINDOW_DAYS")
    
    # URL Shortener
    shortener_base: str = Field(default="", env="SHORTENER_BASE")
    
    # UTM Parameters
    utm_on: bool = Field(default=True, env="UTM_ON")
    utm_source: str = Field(default="telegram", env="UTM_SOURCE")
    utm_medium: str = Field(default="social", env="UTM_MEDIUM")
    utm_campaign: str = Field(default="rss_auto", env="UTM_CAMPAIGN")
    
    # MTProto Configuration (for Stories)
    api_id: Optional[int] = Field(default=None, env="API_ID")
    api_hash: Optional[str] = Field(default=None, env="API_HASH")
    session_enc_key: Optional[str] = Field(default=None, env="SESSION_ENC_KEY")
    
    # Optional Proxy Configuration
    proxy: Optional[str] = Field(default=None, env="PROXY")
    
    # Media Processing
    max_image_size: int = Field(default=1280, env="MAX_IMAGE_SIZE")
    story_image_width: int = Field(default=1080, env="STORY_IMAGE_WIDTH")
    story_image_height: int = Field(default=1920, env="STORY_IMAGE_HEIGHT")
    story_ttl_hours: int = Field(default=24, env="STORY_TTL_HOURS")
    
    # Queue Configuration
    queue_max_size: int = Field(default=1000, env="QUEUE_MAX_SIZE")
    retry_attempts: int = Field(default=3, env="RETRY_ATTEMPTS")
    retry_delay_seconds: int = Field(default=60, env="RETRY_DELAY_SECONDS")
    
    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file: str = Field(default="data/rssbot.log", env="LOG_FILE")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Parse admin_ids from comma-separated string
        if isinstance(self.admin_ids, str):
            self.admin_ids = [int(x.strip()) for x in self.admin_ids.split(",") if x.strip()]
        
        # Parse allow_langs from comma-separated string
        if isinstance(self.allow_langs, str):
            self.allow_langs = [x.strip() for x in self.allow_langs.split(",") if x.strip()]


# Global settings instance
settings = Settings()
