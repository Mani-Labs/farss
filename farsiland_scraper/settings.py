# File: farsiland_scraper/settings.py
# Version: 1.1.1
# Last Updated: 2025-04-16

# Scrapy settings for farsiland_scraper project

import os
import sys
from pathlib import Path

# Add project directory to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from farsiland_scraper.config import DEFAULT_HEADERS

BOT_NAME = 'farsiland_scraper'

SPIDER_MODULES = ['farsiland_scraper.spiders']
NEWSPIDER_MODULE = 'farsiland_scraper.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = DEFAULT_HEADERS["User-Agent"]

# Obey robots.txt rules (set to False for personal scraping)
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests
CONCURRENT_REQUESTS = 8
CONCURRENT_REQUESTS_PER_DOMAIN = 8
DOWNLOAD_DELAY = 0.5  # Delay between requests in seconds
RANDOMIZE_DOWNLOAD_DELAY = True  # Add some randomness to the delay

# Configure item pipelines - UPDATED to include JSONSerializationPipeline
ITEM_PIPELINES = {
    # Run JSON serialization pipeline first to ensure proper data structure
    'farsiland_scraper.pipelines.json_serialization.JSONSerializationPipeline': 100,
    # Then run database pipeline
    'farsiland_scraper.pipelines.save_to_db.SaveToDatabasePipeline': 300,
}

# Set log level
LOG_LEVEL = 'INFO'

# Enable and configure the AutoThrottle extension
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 3.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 4.0
AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600  # 1 hour
HTTPCACHE_DIR = 'httpcache'
HTTPCACHE_IGNORE_HTTP_CODES = [500, 502, 503, 504, 400, 401, 403, 404, 408]
HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Request retry middleware
RETRY_ENABLED = True
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 400, 408, 429]

# Set default request headers
DEFAULT_REQUEST_HEADERS = DEFAULT_HEADERS

# Configure a delay for requests for the same website
DOWNLOAD_TIMEOUT = 30  # seconds

# Cookies
COOKIES_ENABLED = True
COOKIES_DEBUG = False

# Disable Telnet Console
TELNETCONSOLE_ENABLED = False

# Enable memory debugging
MEMDEBUG_ENABLED = False

# Extension settings
EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': None,
    'scrapy.extensions.memusage.MemoryUsage': 1,
    'scrapy.extensions.logstats.LogStats': 1,
}

# Spider middleware
SPIDER_MIDDLEWARES = {
    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 50,
    'scrapy.spidermiddlewares.referer.RefererMiddleware': 700,
    'scrapy.spidermiddlewares.urllength.UrlLengthMiddleware': 800,
    'scrapy.spidermiddlewares.depth.DepthMiddleware': 900,
}

# Downloader middleware
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 500,
    'scrapy.downloadermiddlewares.offsite.OffsiteMiddleware': 550,
    'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 600,
    'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 700,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 750,
    'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 800,
    'scrapy.downloadermiddlewares.stats.DownloaderStats': 850,
    'scrapy.downloadermiddlewares.httpcache.HttpCacheMiddleware': 900,
}

# Stats collection
STATS_CLASS = 'scrapy.statscollectors.MemoryStatsCollector'
