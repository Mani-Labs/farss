import sys

# Force stdout/stderr to UTF-8 so Persian characters can be logged on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")


# File: farsiland_scraper/config.py
# Version: 2.0.0
# Last Updated: 2025-06-01

"""
Configuration settings for Farsiland scraper.

Provides centralized configuration with support for:
- Environment variable overrides
- Docker/NAS environment detection and adaptation
- Resource management (memory, CPU)
- Appropriate path handling for different environments
- Enhanced logging for headless operation
- Persistent and temporary storage locations
"""

import os
import logging
import platform
from logging.handlers import RotatingFileHandler
from pathlib import Path
import tempfile

# Docker environment detection
IN_DOCKER = os.environ.get('FARSILAND_IN_DOCKER', 'false').lower() == 'true'
LOW_MEMORY_MODE = os.environ.get('FARSILAND_LOW_MEMORY', 'false').lower() == 'true'

# Resource limits - useful for constrained environments like NAS
MAX_MEMORY_MB = int(os.environ.get('FARSILAND_MAX_MEMORY_MB', 5120))  # Default 1GB
MAX_CPU_PERCENT = int(os.environ.get('FARSILAND_MAX_CPU_PERCENT', 80))  # Default 80%
MAX_CONCURRENT_PROCESSES = int(os.environ.get('FARSILAND_MAX_CONCURRENT', 4))

# Path Configuration - Different for Docker vs. standard environment
if IN_DOCKER:
    # Docker uses volume mounts
    BASE_DIR = Path('/app')
    DATA_DIR = Path('/data')
    LOG_DIR = Path('/logs')
    CACHE_DIR = Path('/cache')
    # For temporary files
    TEMP_DIR = Path('/tmp/farsiland')
else:
    # Standard environment - based on module location
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = Path(os.environ.get('FARSILAND_DATA_DIR', BASE_DIR / "data"))
    LOG_DIR = Path(os.environ.get('FARSILAND_LOG_DIR', BASE_DIR / "logs"))
    CACHE_DIR = Path(os.environ.get('FARSILAND_CACHE_DIR', BASE_DIR / "cache"))
    TEMP_DIR = Path(os.environ.get('FARSILAND_TEMP_DIR', 
                                   Path(tempfile.gettempdir()) / "farsiland"))

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True, parents=True)
LOG_DIR.mkdir(exist_ok=True, parents=True)
CACHE_DIR.mkdir(exist_ok=True, parents=True)
TEMP_DIR.mkdir(exist_ok=True, parents=True)

# Target website configuration
BASE_URL = os.environ.get('FARSILAND_BASE_URL', "https://farsiland.com")
SITEMAP_URL = os.environ.get('FARSILAND_SITEMAP_URL', f"{BASE_URL}/sitemap_index.xml")
PARSED_SITEMAP_PATH = Path(os.environ.get('FARSILAND_SITEMAP_PATH', DATA_DIR / "parsed_urls.json"))

# Content zone URLs
CONTENT_ZONES = {
    "series": f"{BASE_URL}/series-22/",
    "episodes": f"{BASE_URL}/episodes-12/",
    "movies": f"{BASE_URL}/movies-2025/",
    "iranian_series": f"{BASE_URL}/iranian-series/",
    "old_movies": f"{BASE_URL}/old-iranian-movies/"
}

# Database configuration
DATABASE_PATH = Path(os.environ.get('FARSILAND_DB_PATH', DATA_DIR / "farsiland.db"))
JSON_OUTPUT_PATH = Path(os.environ.get('FARSILAND_JSON_PATH', DATA_DIR / "site_index.json"))

# Authentication configuration
USER_CREDENTIALS = {
    "username": os.environ.get('FARSILAND_USERNAME', ""),
    "password": os.environ.get('FARSILAND_PASSWORD', "")
}

# Default request headers
DEFAULT_HEADERS = {
    "User-Agent": os.environ.get('FARSILAND_USER_AGENT', 
                                 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5,fa;q=0.3",  # Added Farsi language
    "Referer": BASE_URL,
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0"
}

# Request settings - configurable through environment variables
REQUEST_TIMEOUT = int(os.environ.get('FARSILAND_REQUEST_TIMEOUT', 30))  # seconds
REQUEST_RETRY_COUNT = int(os.environ.get('FARSILAND_RETRY_COUNT', 1))
REQUEST_RETRY_DELAY = int(os.environ.get('FARSILAND_RETRY_DELAY', 2))  # seconds

# Proxy settings
PROXY_URL = os.environ.get('FARSILAND_PROXY', None)
PROXY_AUTH = {
    "username": os.environ.get('FARSILAND_PROXY_USER', ""),
    "password": os.environ.get('FARSILAND_PROXY_PASS', "")
}

# Scraping settings
SCRAPE_INTERVAL = int(os.environ.get('FARSILAND_SCRAPE_INTERVAL', 600))  # 10 minutes in seconds
MAX_ITEMS_PER_CATEGORY = int(os.environ.get('FARSILAND_MAX_ITEMS', 20000))
USE_SITEMAP = os.environ.get('FARSILAND_USE_SITEMAP', 'true').lower() == 'true'

# Checkpoint settings
CHECKPOINT_INTERVAL = int(os.environ.get('FARSILAND_CHECKPOINT_INTERVAL', 300))  # 5 minutes
CHECKPOINT_DIR = Path(os.environ.get('FARSILAND_CHECKPOINT_DIR', DATA_DIR / "checkpoints"))
CHECKPOINT_DIR.mkdir(exist_ok=True, parents=True)

# API configuration
API_BASE_URL = os.environ.get('FARSILAND_API_BASE_URL', f"{BASE_URL}/wp-json/wp/v2")
API_TIMEOUT = int(os.environ.get('FARSILAND_API_TIMEOUT', 30))  # seconds
API_RETRIES = int(os.environ.get('FARSILAND_API_RETRIES', 3))
API_USER_AGENT = os.environ.get('FARSILAND_API_USER_AGENT', "FarsilandScraper/4.0")

# API cache configuration
API_CACHE_ENABLED = os.environ.get('FARSILAND_API_CACHE_ENABLED', 'true').lower() == 'true'
API_CACHE_TTL = int(os.environ.get('FARSILAND_API_CACHE_TTL', 3600))  # 1 hour

# Download size limits
MAX_VIDEO_SIZE = int(os.environ.get('FARSILAND_MAX_VIDEO_SIZE', 1024 * 1024 * 500))  # 500 MB
MAX_CONTENT_SIZE = int(os.environ.get('FARSILAND_MAX_CONTENT_SIZE', 1024 * 1024 * 10))  # 10 MB

# API endpoints
API_ENDPOINTS = {
    "movies": "/movies",
    "shows": "/tvshows",
    "episodes": "/episodes"
}

# RSS configuration
RSS_FEEDS = {
    "movies": f"{BASE_URL}/feed?post_type=movies",
    "shows": f"{BASE_URL}/feed?post_type=tvshows",
    "episodes": f"{BASE_URL}/feed?post_type=episodes"
}
RSS_STATE_FILE = os.environ.get('FARSILAND_RSS_STATE_FILE', str(DATA_DIR / "rss_state.json"))
RSS_POLL_INTERVAL = int(os.environ.get('FARSILAND_RSS_POLL_INTERVAL', 600))  # 10 minutes

# Error tracking configuration
ERROR_TRACKER_FILE = os.environ.get('FARSILAND_ERROR_TRACKER_FILE', str(DATA_DIR / "error_patterns.json"))
ERROR_BLACKLIST_THRESHOLD = int(os.environ.get('FARSILAND_ERROR_BLACKLIST_THRESHOLD', 3))

# Configure logging level from environment
LOG_LEVEL_MAP = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}
DEFAULT_LOG_LEVEL = LOG_LEVEL_MAP.get(os.environ.get('FARSILAND_LOG_LEVEL', 'info').lower(), logging.INFO)

def setup_logger(name, log_file, level=None):
    """
    Set up a logger with appropriate handlers for the environment.
    
    Args:
        name: Logger name
        log_file: Path to log file
        level: Logging level (defaults to configured DEFAULT_LOG_LEVEL)
        
    Returns:
        Configured logger instance
    """
    if level is None:
        level = DEFAULT_LOG_LEVEL
        
    logger = logging.getLogger(name)
    
    # If logger already has handlers, return it to avoid duplicates
    if logger.handlers:
        return logger
        
    logger.setLevel(level)

    # File handler with rotation
    file_handler = RotatingFileHandler(
        LOG_DIR / log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Only add console handler if not in Docker or if explicitly requested
    if not IN_DOCKER or os.environ.get('FARSILAND_CONSOLE_LOG', 'false').lower() == 'true':
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] [%(name)s] %(message)s', datefmt='%H:%M:%S')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger

# Create main logger
LOGGER = setup_logger('farsiland_scraper', 'scraper.log')

# System information logging
def log_system_info():
    """Log system information for debugging."""
    try:
        system_info = {
            "os": platform.system(),
            "python": platform.python_version(),
            "platform": platform.platform(),
            "memory_limit": f"{MAX_MEMORY_MB}MB",
            "cpu_limit": f"{MAX_CPU_PERCENT}%",
            "docker": IN_DOCKER,
            "low_memory": LOW_MEMORY_MODE,
        }
        
        LOGGER.info("System information:")
        for key, value in system_info.items():
            LOGGER.info(f"  {key}: {value}")
            
        # Log directory paths
        LOGGER.info("Directory paths:")
        LOGGER.info(f"  BASE_DIR: {BASE_DIR}")
        LOGGER.info(f"  DATA_DIR: {DATA_DIR}")
        LOGGER.info(f"  LOG_DIR: {LOG_DIR}")
        LOGGER.info(f"  CACHE_DIR: {CACHE_DIR}")
        LOGGER.info(f"  TEMP_DIR: {TEMP_DIR}")
        
        # Try to detect available memory if psutil is installed
        try:
            import psutil
            mem = psutil.virtual_memory()
            LOGGER.info(f"Available memory: {mem.available / (1024*1024):.1f}MB of {mem.total / (1024*1024):.1f}MB")
        except ImportError:
            LOGGER.debug("psutil not available, skipping memory detection")
    except Exception as e:
        LOGGER.error(f"Error logging system info: {e}")

# Call system info logging when module is imported (if in verbose mode)
if DEFAULT_LOG_LEVEL == logging.DEBUG:
    log_system_info()