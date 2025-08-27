#!/usr/bin/env python3
"""
RSS Bot - Telegram RSS Aggregator
Main entry point
"""
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.bot import RSSBot
from src.scheduler import RSSScheduler
from src.database import create_engine_and_session, create_tables
from src.config import settings

# Configure logging
def setup_logging():
    """Setup logging configuration"""
    log_level = getattr(logging, settings.log_level.upper())
    
    # Create logs directory
    log_dir = Path(settings.log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(settings.log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

async def main():
    """Main application entry point"""
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting RSS Bot...")
    
    # Initialize database
    try:
        engine, SessionLocal = create_engine_and_session()
        create_tables(engine)
        logger.info("Database initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return 1
    
    # Initialize bot and scheduler
    bot = None
    scheduler = None
    
    try:
        # Initialize bot
        bot = RSSBot()
        await bot.initialize()
        
        # Initialize scheduler
        scheduler = RSSScheduler(bot.publisher)
        await scheduler.start()
        
        # Start bot
        await bot.start()
        
        logger.info("RSS Bot started successfully")
        
        # Keep running
        while True:
            await asyncio.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        return 1
    finally:
        # Cleanup
        if scheduler:
            scheduler.stop()
        
        if bot:
            await bot.stop()
        
        logger.info("RSS Bot stopped")
    
    return 0

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger = logging.getLogger(__name__)
    logger.info(f"Received signal {signum}, shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run main loop
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
