# File: farsiland_scraper/run.py
# Version: 6.0.0
# Last Updated: 2025-04-27

"""
Main execution module for Farsiland scraper.

Handles:
1. Command-line interface and argument parsing
2. Orchestration of spider execution
3. Incremental update detection and filtering
4. Resource monitoring and management
5. Session state persistence
6. Error detection and recovery
7. Proper shutdown and cleanup

Changelog:
- [6.0.0] Complete rewrite with improved architecture
- [6.0.0] Fixed sitemap parser return handling with robust type validation
- [6.0.0] Implemented thread-safe resource monitoring
- [6.0.0] Enhanced error handling with structured logging
- [6.0.0] Added comprehensive URL normalization
- [6.0.0] Improved memory management for Docker/NAS environments
- [6.0.0] Enhanced checkpoint integration for resumable crawls
- [6.0.0] Fixed incremental update handling with consistent date comparison
- [6.0.0] Added atomic file operations for improved reliability
- [5.0.0] Previous major version
"""

import os
import sys
import time
import json
import argparse
import datetime
import logging
import signal
import re
import importlib
import gc
import traceback
import unicodedata
import threading
import tempfile
from urllib.parse import urlparse, urlunparse, quote, unquote
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple, Union

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import Scrapy components
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scrapy.utils.project import get_project_settings

# Import project components
from farsiland_scraper.config import (
    LOGGER, 
    SCRAPE_INTERVAL, 
    PARSED_SITEMAP_PATH,
    MAX_ITEMS_PER_CATEGORY,
    RSS_FEEDS,
    RSS_STATE_FILE,
    RSS_POLL_INTERVAL,
    API_BASE_URL,
    MAX_MEMORY_MB,
    MAX_CPU_PERCENT,
    MAX_CONCURRENT_PROCESSES,
    IN_DOCKER,
    TEMP_DIR
)

# Check for psutil availability
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    LOGGER.warning("psutil package not available, resource monitoring disabled")

# Define constants
DEFAULT_SPIDER_MODULES = ["farsiland_scraper.spiders"]
SPIDER_TYPES = {
    'series': 'SeriesSpider',
    'episodes': 'EpisodesSpider',
    'movies': 'MoviesSpider',
    'all': None  # Special value meaning "all spiders"
}

# URL patterns for content type detection
CONTENT_PATTERNS = {
    'movies': r"https?://[^/]+/movies/[^/]+/?$",
    'episodes': r"https?://[^/]+/episodes/[^/]+/?$", 
    'series': r"https?://[^/]+/tvshows/[^/]+/?$"
}

class ResourceMonitor:
    """Thread-safe resource monitoring and management."""
    
    def __init__(self, logger=LOGGER, check_interval=60):
        """
        Initialize resource monitor.
        
        Args:
            logger: Logger instance
            check_interval: Minimum seconds between checks
        """
        self._lock = threading.Lock()
        self.last_check_time = 0
        self.check_interval = check_interval
        self.logger = logger
        
    def can_check_resources(self, force=False) -> bool:
        """
        Determine if a resource check can be performed.
        
        Args:
            force: Force check regardless of interval
            
        Returns:
            True if check is allowed
        """
        with self._lock:
            current_time = time.time()
            if force or (current_time - self.last_check_time >= self.check_interval):
                self.last_check_time = current_time
                return True
            return False
            
    def check_resources(self, force=False, low_memory_mode=False) -> Dict[str, Any]:
        """
        Check system resources and take action if needed.
        
        Args:
            force: Force check regardless of interval
            low_memory_mode: Enable aggressive memory management
            
        Returns:
            Dictionary with resource stats
        """
        if not PSUTIL_AVAILABLE:
            return {"error": "psutil not available"}
            
        if not self.can_check_resources(force):
            return {"status": "skipped", "reason": "interval not reached"}
            
        try:
            process = psutil.Process()
            
            # Memory usage
            memory_info = process.memory_info()
            memory_percent = process.memory_percent()
            
            # CPU usage
            cpu_percent = process.cpu_percent(interval=0.1)
            
            stats = {
                "memory_mb": memory_info.rss / (1024*1024),
                "memory_percent": memory_percent,
                "cpu_percent": cpu_percent,
                "timestamp": time.time()
            }
            
            # Take action if resources are critically high
            if memory_percent > 90:
                self.logger.warning(f"Critical memory usage: {memory_percent:.1f}%")
                self._trigger_memory_relief(aggressive=True)
                stats["action"] = "aggressive_memory_relief"
            elif memory_percent > 75:
                self.logger.warning(f"High memory usage: {memory_percent:.1f}%")
                self._trigger_memory_relief(aggressive=low_memory_mode)
                stats["action"] = "memory_relief"
                
            if cpu_percent > MAX_CPU_PERCENT:
                self.logger.warning(f"High CPU usage: {cpu_percent:.1f}%")
                self._throttle_processes()
                stats["action"] = "cpu_throttling"
                
            return stats
        except Exception as e:
            self.logger.error(f"Error checking resources: {e}")
            return {"error": str(e)}
            
    def _trigger_memory_relief(self, aggressive=False) -> None:
        """
        Implement memory relief strategies.
        
        Args:
            aggressive: Use aggressive memory cleanup
        """
        # Force garbage collection
        gc.collect()
        
        if aggressive:
            # More aggressive memory relief
            if 'api_client' in sys.modules:
                # If API client is loaded, try to clear its cache
                try:
                    from farsiland_scraper.core.api_client import APIClient
                    for obj in gc.get_objects():
                        if isinstance(obj, APIClient):
                            obj.clear_cache()
                            self.logger.info("Cleared API client cache")
                except (ImportError, Exception) as e:
                    self.logger.warning(f"Error clearing API cache: {e}")
            
            # Clear any temporary files
            try:
                temp_files = list(TEMP_DIR.glob("*.tmp"))
                for file in temp_files:
                    try:
                        os.unlink(file)
                    except OSError:
                        pass
                if temp_files:
                    self.logger.info(f"Cleared {len(temp_files)} temporary files")
            except Exception as e:
                self.logger.warning(f"Error clearing temporary files: {e}")
    
    def _throttle_processes(self) -> None:
        """Implement CPU throttling strategies."""
        # Add sleep to reduce CPU usage
        time.sleep(0.5)


class ScrapeManager:
    """
    Manager class for controlling the scraping process.
    Handles initialization, command processing, and orchestration.
    """

    def __init__(self, args: argparse.Namespace):
        """Initialize the scrape manager with parsed arguments."""
        self.args = args
        self.process = None
        self.start_time = None
        self.interrupted = False
        
        # Resource monitoring
        self.resource_monitor = ResourceMonitor()

        # Configure signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_interrupt)
        signal.signal(signal.SIGTERM, self.handle_interrupt)

        # Setup logging
        self.setup_logging()
        
        # Initialize crawler settings
        self.settings = self.load_scrapy_settings()
        
        # Initialize database
        self._init_database()
        
        # Import needed components
        self._import_components()
        
        # Initialize checkpoint manager
        self.checkpoint_manager = None
        if hasattr(self.args, 'checkpoint_interval') and self.args.checkpoint_interval > 0:
            try:
                from farsiland_scraper.utils.checkpoint_manager import CheckpointManager
                self.checkpoint_manager = CheckpointManager(
                    checkpoint_interval=self.args.checkpoint_interval,
                    item_threshold=self.args.checkpoint_items or 1000
                )
                LOGGER.info(f"Checkpoint manager initialized with interval: {self.args.checkpoint_interval}s")
            except ImportError:
                LOGGER.warning("CheckpointManager not available, checkpointing disabled")
        
        # Initialize API client if using API-first approach
        self.api_client = None
        if args.api_first:
            try:
                from farsiland_scraper.core.api_client import APIClient
                self.api_client = APIClient(base_url=API_BASE_URL)
                LOGGER.info("Using API-first approach")
            except ImportError:
                LOGGER.error("APIClient not available, falling back to HTML-only mode")
                self.api_first = False
        else:
            LOGGER.info("Using HTML-first approach")
        
        # Initialize error tracker
        self.error_tracker = None
        try:
            from farsiland_scraper.utils.error_tracker import ErrorTracker
            self.error_tracker = ErrorTracker()
            LOGGER.info("Error tracker initialized")
        except ImportError:
            LOGGER.warning("ErrorTracker not available, error tracking disabled")
        
        # Initialize RSS monitor if enabled
        self.rss_monitor = None
        if args.rss:
            try:
                from farsiland_scraper.core.rss_monitor import RSSMonitor
                self.rss_monitor = RSSMonitor(
                    feeds=RSS_FEEDS,
                    state_file=args.rss_state_file or RSS_STATE_FILE,
                    api_client=self.api_client,
                    error_tracker=self.error_tracker
                )
                LOGGER.info(f"RSS monitoring enabled with interval: {args.rss_interval}s")
            except ImportError:
                LOGGER.warning("RSSMonitor not available, RSS monitoring disabled")
        
        # Initialize session manager for authentication
        self.session_manager = None
        if hasattr(args, 'auth') and args.auth:
            try:
                from farsiland_scraper.auth.session_manager import SessionManager
                self.session_manager = SessionManager()
                LOGGER.info("Session manager initialized for authentication")
            except ImportError:
                LOGGER.warning("SessionManager not available, authentication disabled")
        
        # Parse modified_after date if provided
        self.modified_after = None
        if args.since:
            try:
                self.modified_after = datetime.datetime.fromisoformat(args.since)
                LOGGER.info(f"Will only process content modified after: {args.since}")
            except ValueError:
                LOGGER.error(f"Invalid date format: {args.since}, use ISO format (YYYY-MM-DD)")
        elif args.modified_window:
            days = int(args.modified_window)
            self.modified_after = datetime.datetime.now() - datetime.timedelta(days=days)
            LOGGER.info(f"Will only process content modified in the last {days} days")

        # Now that modified_after exists, log execution mode
        self._log_execution_mode()

        # Initialize sitemap data
        self.sitemap_data = {}
        self.sitemap_modified_data = {}
        
        try:
            # Handle specific URL if provided
            if args.url:
                self.sitemap_data = self._create_sitemap_from_url(args.url)
                self.sitemap_modified_data = self.sitemap_data  # For single URL, all data is considered modified
                # Force sitemap mode when using specific URL
                self.args.sitemap = True
            else:
                # Load sitemap data if specified
                if args.sitemap or args.sitemap_only:
                    sitemap_result = self.load_sitemap_data()
                    self.sitemap_data, self.sitemap_modified_data = self._validate_sitemap_result(sitemap_result)
                    
                    # Log the sitemap stats more clearly
                    self._log_sitemap_stats()
        except Exception as e:
            LOGGER.error(f"Error initializing sitemap data: {e}", exc_info=True)
            self.sitemap_data = {}
            self.sitemap_modified_data = {}
            
    def _validate_sitemap_result(self, result: Any) -> Tuple[Dict, Dict]:
        """
        Safely validate and process sitemap parsing results.
        
        Args:
            result: Result from sitemap parser
            
        Returns:
            Tuple of (all_urls, modified_urls) dictionaries
        """
        if isinstance(result, dict):
            # New format with metadata
            if "all_urls" in result and "modified_urls" in result:
                return result["all_urls"], result.get("modified_urls", {})
            
            # Old format or simple dictionary
            return result, {}
        elif isinstance(result, bool) and result:
            # Boolean success value without data
            LOGGER.warning("Sitemap parsing returned boolean success without data")
            return {}, {}
        elif result is None:
            LOGGER.error("Sitemap parsing returned None")
            return {}, {}
        else:
            LOGGER.error(f"Invalid sitemap parsing result type: {type(result)}")
            return {}, {}
            
    def _import_components(self):
        """Import needed components based on arguments."""
        # Import Database
        try:
            from farsiland_scraper.database.models import Database
            self.Database = Database
        except ImportError:
            self.Database = None
            LOGGER.warning("Database module not available, DB features disabled")
            
        # Import NewItemTracker
        try:
            from farsiland_scraper.utils.new_item_tracker import NewItemTracker
            self.NewItemTracker = NewItemTracker
        except ImportError:
            self.NewItemTracker = None
            LOGGER.warning("NewItemTracker not available, new item tracking disabled")
            
        # Import SitemapParser
        try:
            from farsiland_scraper.utils.sitemap_parser import SitemapParser
            self.SitemapParser = SitemapParser
        except ImportError:
            self.SitemapParser = None
            LOGGER.error("SitemapParser not available, sitemap functionality disabled")
            
        # Import spider classes
        try:
            from farsiland_scraper.spiders.series_spider import SeriesSpider
            from farsiland_scraper.spiders.episodes_spider import EpisodesSpider
            from farsiland_scraper.spiders.movies_spider import MoviesSpider
            
            self.spider_classes = {
                'series': SeriesSpider,
                'episodes': EpisodesSpider,
                'movies': MoviesSpider
            }
        except ImportError as e:
            LOGGER.error(f"Error importing spider classes: {e}")
            self.spider_classes = {}
            
    def _init_database(self):
        """Initialize database connection."""
        try:
            from farsiland_scraper.database.models import Database
            self.db = Database()
            LOGGER.info("Database initialized")
            
            # Optional: Run optimizations if specified
            if hasattr(self.args, 'optimize_db') and self.args.optimize_db:
                LOGGER.info("Running database optimization...")
                if hasattr(self.db, 'optimize_database'):
                    success = self.db.optimize_database()
                    if success:
                        LOGGER.info("Database optimization completed successfully")
                    else:
                        LOGGER.warning("Database optimization failed")
                else:
                    LOGGER.warning("Database optimization not supported")
                    
        except ImportError:
            self.db = None
            LOGGER.warning("Database module not available, DB features disabled")
        except Exception as e:
            self.db = None
            LOGGER.error(f"Error initializing database: {e}")
            
    def _log_execution_mode(self):
        """Log the current execution mode and important settings."""
        LOGGER.info("=== Farsiland Scraper Execution Mode ===")
        LOGGER.info(f"Data Source:   {'API-first with HTML fallback' if self.args.api_first else 'HTML-first'}")
        LOGGER.info(f"URL Source:    {'Sitemap' if self.args.sitemap else 'Default crawl URLs'}")
        LOGGER.info(f"Sitemap Only:  {'Yes - will not fetch pages' if self.args.sitemap_only else 'No - will fetch pages'}")
        LOGGER.info(f"RSS Enabled:   {'Yes' if self.args.rss else 'No'}")
        LOGGER.info(f"Daemon Mode:   {'Yes' if self.args.daemon else 'No'}")
        LOGGER.info(f"Force Refresh: {'Yes' if self.args.force_refresh else 'No'}")
        LOGGER.info(f"Max Items:     {self.args.limit if self.args.limit else MAX_ITEMS_PER_CATEGORY}")
        LOGGER.info(f"Spiders:       {', '.join(self.args.spiders)}")
        
        # Log incremental update settings
        if self.modified_after:
            LOGGER.info(f"Incremental:   Yes - since {self.modified_after.isoformat()}")
        else:
            LOGGER.info(f"Incremental:   No - full crawl")
            
        # Log Docker/NAS settings if applicable
        if IN_DOCKER:
            LOGGER.info(f"Environment:   Docker container")
            LOGGER.info(f"Resource Limits: {MAX_MEMORY_MB}MB RAM, {MAX_CPU_PERCENT}% CPU, {MAX_CONCURRENT_PROCESSES} processes")
            
        if self.args.url:
            LOGGER.info(f"Single URL:    {self.args.url}")
        LOGGER.info("=====================================")
        
    def _log_sitemap_stats(self):
        """Log detailed statistics about the loaded sitemap."""
        try:
            if not self.sitemap_data:
                LOGGER.info("No sitemap data loaded.")
                return
                
            LOGGER.info("=== Sitemap Statistics ===")
            
            # Track counts by category
            category_stats = {}
            total_urls = 0
            total_modified = 0
            
            for category, urls in self.sitemap_data.items():
                if not isinstance(urls, list):
                    LOGGER.warning(f"Invalid URL list type for category {category}: {type(urls)}")
                    continue
                    
                url_count = len(urls)
                total_urls += url_count
                
                # Count modified URLs for this category
                modified_count = 0
                if category in self.sitemap_modified_data:
                    modified_data = self.sitemap_modified_data[category]
                    if isinstance(modified_data, list):
                        modified_count = len(modified_data)
                        total_modified += modified_count
                    
                category_stats[category] = {
                    'total': url_count,
                    'modified': modified_count
                }
                
                # Log category information
                LOGGER.info(f"{category.capitalize()}: {url_count} total URLs, {modified_count} modified")
                
                # Log sample URLs as examples (up to 3)
                if url_count > 0:
                    LOGGER.info(f"  Sample URLs for {category}:")
                    # Get first 3 items safely
                    sample_urls = list(urls)[:3] if isinstance(urls, (set, dict)) else urls[:3]
                    for i, entry in enumerate(sample_urls):
                        url = entry['url'] if isinstance(entry, dict) else entry
                        LOGGER.info(f"  {i+1}. {url}")
                    
            # Log overall statistics
            if self.modified_after:
                LOGGER.info(f"Modified URLs: {total_modified} (since {self.modified_after.isoformat()})")
                
            LOGGER.info(f"Total URLs: {total_urls}")
            LOGGER.info("=========================")
            
        except Exception as e:
            LOGGER.error(f"Error logging sitemap statistics: {e}")

    def normalize_url(self, url: str) -> str:
        """
        Normalize URL with proper handling of non-ASCII characters.
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL
        """
        if not url:
            return ""
            
        try:
            # Parse URL components
            parsed = urlparse(url)
            
            # Normalize path - ensure proper encoding of non-ASCII characters
            path = unquote(parsed.path)
            
            # Convert to normalized Unicode form
            path = unicodedata.normalize('NFKC', path)
            
            # Convert Persian/Arabic digits to Latin
            persian_digits = {
                '۰': '0', '۱': '1', '۲': '2', '۳': '3', '۴': '4',
                '۵': '5', '۶': '6', '۷': '7', '۸': '8', '۹': '9'
            }
            for persian, latin in persian_digits.items():
                path = path.replace(persian, latin)
            
            # Reconstruct URL
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc,
                path.rstrip('/'),  # Remove trailing slash
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
            
            return normalized
        except Exception as e:
            LOGGER.warning(f"Error normalizing URL {url}: {e}")
            # On error, just strip trailing slash as minimal normalization
            return url.rstrip('/')

    def _create_sitemap_from_url(self, url: str) -> Dict[str, List[Dict[str, str]]]:
        """
        Create a sitemap dictionary from a specific URL.
        Determines the content type based on URL pattern.
        
        Args:
            url: The URL to create a sitemap entry for
            
        Returns:
            A dictionary with content type keys and URL entry lists
        """
        # Initialize empty sitemap
        sitemap_data = {
            "movies": [],
            "episodes": [],
            "series": []
        }
        
        # Normalize the URL
        url = self.normalize_url(url)
        
        # Determine content type based on URL pattern
        content_type = self.detect_content_type(url)
        
        if content_type:
            # Add the URL to the appropriate content type
            sitemap_data[content_type].append({"url": url})
            LOGGER.info(f"Created sitemap with 1 {content_type} URL: {url}")
        else:
            LOGGER.warning(f"Could not determine content type for URL: {url}, skipping")
            
        return sitemap_data
    
    def detect_content_type(self, url: str) -> Optional[str]:
        """
        Detect content type based on URL pattern.
        
        Args:
            url: URL to analyze
            
        Returns:
            Content type string or None if can't be determined
        """
        if not url:
            return None
            
        # Normalize URL for consistent matching
        normalized_url = self.normalize_url(url)
            
        # Check against patterns
        for content_type, pattern in CONTENT_PATTERNS.items():
            if re.match(pattern, normalized_url):
                return content_type
                
        # If no direct match, try to infer from URL segments
        url_parts = normalized_url.lower()
        if "/movies/" in url_parts:
            return "movies"
        elif "/episodes/" in url_parts:
            return "episodes"
        elif "/tvshows/" in url_parts or "/series" in url_parts:
            return "series"
            
        # Default to None if can't determine
        return None
    
    def setup_logging(self):
        """Configure logging for the scrape manager."""
        log_level = logging.DEBUG if self.args.verbose else logging.INFO
        LOGGER.setLevel(log_level)

        # Log to file if specified
        if self.args.log_file:
            try:
                log_dir = os.path.dirname(self.args.log_file)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)

                file_handler = logging.FileHandler(self.args.log_file)
                file_formatter = logging.Formatter(
                    '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
                )
                file_handler.setFormatter(file_formatter)
                LOGGER.addHandler(file_handler)
            except Exception as e:
                LOGGER.error(f"Failed to setup file logging: {e}")

    def load_scrapy_settings(self) -> Settings:
        """
        Load Scrapy settings from the project.
        
        Returns:
            Settings: Scrapy settings object
        """
        try:
            settings = Settings()
            project_settings = get_project_settings()

            for key, value in project_settings.items():
                settings.set(key, value)

            # Apply custom settings from arguments
            settings.set('LOG_LEVEL', 'DEBUG' if self.args.verbose else 'INFO')

            if self.args.concurrent_requests:
                settings.set('CONCURRENT_REQUESTS', self.args.concurrent_requests)

            if self.args.download_delay:
                settings.set('DOWNLOAD_DELAY', self.args.download_delay)
                
            # Handle force refresh
            if self.args.force_refresh:
                settings.set('HTTPCACHE_ENABLED', False)
                
            # Add environment-specific settings
            if IN_DOCKER:
                # Optimize for Docker environments
                settings.set('REACTOR_THREADPOOL_MAXSIZE', MAX_CONCURRENT_PROCESSES)
                settings.set('COOKIES_ENABLED', True)
                settings.set('RETRY_ENABLED', True)
                settings.set('DOWNLOAD_TIMEOUT', 60)
                settings.set('REDIRECT_MAX_TIMES', 5)
                
            # Add memory management settings
            if hasattr(self.args, 'low_memory') and self.args.low_memory:
                LOGGER.info("Low memory mode enabled")
                settings.set('CONCURRENT_REQUESTS', min(settings.get('CONCURRENT_REQUESTS', 16), 4))
                settings.set('MEMUSAGE_WARNING_MB', MAX_MEMORY_MB * 0.7)
                settings.set('MEMUSAGE_LIMIT_MB', MAX_MEMORY_MB * 0.9)
                settings.set('JOBDIR', str(TEMP_DIR / "scrapy-jobs"))

            return settings
        except Exception as e:
            LOGGER.error(f"Error loading Scrapy settings: {e}")
            # Return default settings as fallback
            return Settings()

    def _is_valid_content_url(self, url: str, spider_type: str) -> bool:
        """
        Check if a URL is a valid content page for a specific spider type.
        
        Args:
            url: URL to check
            spider_type: Type of spider ('series', 'episodes', 'movies')
        
        Returns:
            bool: True if the URL is a valid content page
        """
        if not url:
            return False
            
        # Normalize URL for consistent matching
        normalized_url = self.normalize_url(url)
            
        # Use content patterns dictionary for validation
        pattern = CONTENT_PATTERNS.get(spider_type)
        if not pattern:
            return False
            
        is_valid = bool(re.match(pattern, normalized_url))
        
        if not is_valid:
            LOGGER.debug(f"Invalid URL for {spider_type}: {normalized_url}")
            
        return is_valid

    def load_sitemap_data(self) -> Dict:
        """
        Load parsed sitemap data from JSON file.
        
        Returns:
            Dict containing URLs for different content types or dict with
            structured format {'all_urls': {...}, 'modified_urls': {...}}
        """
        sitemap_path = self.args.sitemap_file or PARSED_SITEMAP_PATH

        if not os.path.exists(sitemap_path):
            LOGGER.error(f"Sitemap file not found: {sitemap_path}")
            if self.args.update_sitemap:
                try:
                    # Ensure the directory exists
                    os.makedirs(os.path.dirname(sitemap_path), exist_ok=True)
                    
                    LOGGER.info("Updating sitemap data...")
                    
                    # Make sure SitemapParser is available
                    if not self.SitemapParser:
                        LOGGER.error("SitemapParser not available, cannot update sitemap")
                        return {}
                        
                    parser = self.SitemapParser(
                        output_file=sitemap_path,
                        error_tracker=self.error_tracker
                    )
                    
                    # Pass modified_after if we're doing incremental updates
                    if self.modified_after:
                        # Request IDs instead of full URLs if using API-first
                        result = parser.run(
                            modified_after=self.modified_after,
                            return_ids=self.args.api_first
                        )
                    else:
                        result = parser.run()
                        
                    if result:
                        LOGGER.info(f"Sitemap updated successfully: {sitemap_path}")
                        return result
                    else:
                        LOGGER.error("Failed to update sitemap")
                        return {}
                except Exception as e:
                    LOGGER.error(f"Error updating sitemap: {e}")
                    return {}
            else:
                LOGGER.error("Use --update-sitemap to generate the sitemap file")
                return {}

        try:
            with open(sitemap_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Check if this is the new format (with metadata and modified URLs)
            if "metadata" in data:
                # New format with separate all_urls and modified_urls containers
                all_urls = {}
                modified_urls = {}
                
                # Process standard content categories
                for category in ["movies", "episodes"]:
                    if category in data:
                        all_urls[category] = data[category]
                    if f"modified_{category}" in data:
                        modified_urls[category] = data[f"modified_{category}"]
                
                # Handle "shows" category which might be stored as "series"
                if "shows" in data:
                    all_urls["shows"] = data["shows"]
                elif "series" in data:
                    all_urls["series"] = data["series"]
                    
                if "modified_shows" in data:
                    modified_urls["shows"] = data["modified_shows"]
                elif "modified_series" in data:
                    modified_urls["shows"] = data["modified_series"]
                    
                # Return structured result
                return {
                    "all_urls": all_urls,
                    "modified_urls": modified_urls
                }
            else:
                # Old format - backward compatibility
                sitemap_data = {}
                
                # Process movies and episodes
                for category in ["movies", "episodes"]:
                    if category in data:
                        sitemap_data[category] = data[category]
                
                # Handle "shows" which might be stored as "series"
                if "shows" in data:
                    sitemap_data["shows"] = data["shows"]
                elif "series" in data:
                    sitemap_data["series"] = data["series"]
                    
                # Print stats
                for category, urls in sitemap_data.items():
                    LOGGER.info(f"Loaded {len(urls)} URLs for {category} from sitemap")

                return sitemap_data
                
        except json.JSONDecodeError as e:
            LOGGER.error(f"Invalid JSON in sitemap file: {e}")
            return {}
        except Exception as e:
            LOGGER.error(f"Error loading sitemap data: {e}")
            return {}

    def get_start_urls(self, spider_type: str) -> List[str]:
        """
        Get start URLs for a specific spider type with robust validation.
        
        Args:
            spider_type: Type of spider ('series', 'episodes', 'movies')
            
        Returns:
            List of start URLs
        """
        if not self.args.sitemap and not self.args.sitemap_only:
            # Without sitemap, spiders use their default start URLs
            LOGGER.info(f"No sitemap specified, returning empty start_urls for {spider_type}")
            return []

        # Map spider type to sitemap category
        category = 'series' if spider_type == 'series' else spider_type

        # Get URLs based on incremental mode
        urls = []
        if self.modified_after and self.sitemap_modified_data:
            # Check if category exists in modified data
            if category in self.sitemap_modified_data:
                urls = self.sitemap_modified_data.get(category, [])
                LOGGER.info(f"Using {len(urls)} modified {category} URLs from sitemap (incremental mode)")
            else:
                LOGGER.warning(f"No modified URLs found for {category}, using all URLs instead")
                urls = self.sitemap_data.get(category, [])
        else:
            # Use all URLs if not in incremental mode
            urls = self.sitemap_data.get(category, [])
            LOGGER.info(f"Using all {len(urls)} {category} URLs from sitemap")
            
        # Extract URL strings with proper validation
        start_urls = []
        if urls:
            for entry in urls:
                if isinstance(entry, dict) and 'url' in entry:
                    start_urls.append(entry["url"])
                elif isinstance(entry, str):
                    start_urls.append(entry)
                else:
                    LOGGER.warning(f"Invalid URL entry type: {type(entry)}")
            
        # Filter URLs to include only valid content pages
        original_count = len(start_urls)
        valid_urls = [url for url in start_urls if self._is_valid_content_url(url, spider_type)]
        filtered_count = len(valid_urls)
        
        if filtered_count < original_count:
            LOGGER.info(f"Filtered {original_count - filtered_count} non-content URLs from {category}")
        
        # Apply limit if specified
        max_items = self.get_max_items()
        if max_items > 0:
            valid_urls = valid_urls[:max_items]
            LOGGER.info(f"Limiting {category} URLs to {len(valid_urls)} (limit={max_items})")

        LOGGER.info(f"Returning {len(valid_urls)} start_urls for {spider_type}")
        return valid_urls
        
    def get_max_items(self) -> int:
        """
        Get the maximum number of items to process.
        
        Returns:
            int: Maximum number of items to process
        """
        if self.args.limit and self.args.limit > 0:
            return self.args.limit
        return MAX_ITEMS_PER_CATEGORY  # Default from config

    def create_crawler_process(self) -> CrawlerProcess:
        """
        Create a Scrapy crawler process.
        
        Returns:
            CrawlerProcess: Configured crawler process
        """
        try:
            return CrawlerProcess(self.settings)
        except Exception as e:
            LOGGER.error(f"Error creating crawler process: {e}")
            raise

    def run_spiders(self) -> bool:
        """
        Run the specified spiders with comprehensive error handling.
        
        Returns:
            bool: True if successful, False otherwise
        """
        # If sitemap-only mode is enabled, skip running spiders
        if self.args.sitemap_only:
            LOGGER.info("Running in sitemap-only mode, skipping spider execution")
            return True
            
        self.start_time = time.time()
        LOGGER.info(f"Starting crawl at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            # Monitor initial memory usage
            self.resource_monitor.check_resources(force=True, low_memory_mode=getattr(self.args, 'low_memory', False))
            
            # Create crawler process
            self.process = self.create_crawler_process()

            # Determine which spiders to run
            spider_types = self.determine_spider_types()
            if not spider_types:
                LOGGER.error("No valid spider types to run")
                return False

            # Configure and add spiders to the process
            for spider_type in spider_types:
                try:
                    self.add_spider_to_process(spider_type)
                except Exception as e:
                    LOGGER.error(f"Error adding spider {spider_type}: {e}")
                    continue

            # Start the crawling process
            LOGGER.info(f"Starting crawl with spiders: {', '.join(spider_types)}")
            self.process.start()

            if not self.interrupted:
                LOGGER.info("Crawling finished successfully")
                
                # Final resource check
                self.resource_monitor.check_resources(force=True, low_memory_mode=getattr(self.args, 'low_memory', False))
                
                return True
            else:
                LOGGER.warning("Crawling was interrupted")
                return False

        except Exception as e:
            LOGGER.error(f"Error running spiders: {e}", exc_info=True)
            return False
        finally:
            elapsed_time = time.time() - self.start_time
            LOGGER.info(f"Total crawl time: {elapsed_time:.2f} seconds")
            
            # Force garbage collection
            gc.collect()
            
    def determine_spider_types(self) -> List[str]:
        """
        Determine which spider types to run.
        
        Returns:
            List of spider type names
        """
        if self.args.url:
            # When using --url, run only the spider for the appropriate content type
            # Find which content type has URLs in the sitemap
            spider_types = []
            for spider_type, urls in self.sitemap_data.items():
                if urls:
                    spider_types.append(spider_type)
            return spider_types
        else:
            # Otherwise use the specified spiders
            if 'all' in self.args.spiders:
                return ['series', 'episodes', 'movies']
            return [t for t in self.args.spiders if t in SPIDER_TYPES]
            
    def add_spider_to_process(self, spider_type: str) -> None:
        """
        Add a spider to the crawler process.
        
        Args:
            spider_type: Type of spider to add
        """
        # Get the appropriate spider class
        spider_class = self.spider_classes.get(spider_type)
        if not spider_class:
            LOGGER.warning(f"Spider class not found for type: {spider_type}")
            return

        # Get start URLs for this spider
        start_urls = self.get_start_urls(spider_type)

        # Get max items
        max_items = self.get_max_items()

        # Spider-specific settings
        spider_settings = {
            'start_urls': start_urls,
            'max_items': max_items,
            'export_json': self.args.export,
            'api_first': self.args.api_first,
            'modified_after': self.modified_after,
            'error_tracker': self.error_tracker,
            'checkpoint_manager': self.checkpoint_manager,
            'low_memory': getattr(self.args, 'low_memory', False),
            'validate_api_data': True,
            'html_fallback': True 
            
        }
        
        # Pass max_video_size if available
        if hasattr(self.args, 'max_video_size') and self.args.max_video_size:
            spider_settings['max_video_size'] = self.args.max_video_size

        # Add the spider to the process
        self.process.crawl(spider_class, **spider_settings)
        LOGGER.info(f"Added {spider_type} spider to crawl queue with {len(start_urls)} URLs and max_items={max_items}")

    def process_new_content(self) -> bool:
        """
        Process newly discovered content.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.args.notify and not self.args.export:
            return True

        try:
            # Make sure we have database and NewItemTracker
            if not self.db or not self.NewItemTracker:
                LOGGER.error("Database or NewItemTracker not available")
                return False
            
            # Export to JSON if requested
            if self.args.export:
                LOGGER.info("Exporting database to JSON...")
                success = self.db.export_to_json(output_path=self.args.export_file)
                if success:
                    LOGGER.info("Database exported successfully")
                else:
                    LOGGER.error("Failed to export database")

            # Process and notify about new content if requested
            if self.args.notify:
                LOGGER.info("Processing new content for notifications...")
                tracker = self.NewItemTracker(self.db)
                new_content = tracker.get_new_content()

                if any(new_content.values()):
                    LOGGER.info(
                        f"Found new content: {', '.join(f'{k}: {len(v)}' for k, v in new_content.items() if v)}"
                    )
                    tracker.notify_new_content(new_content)
                else:
                    LOGGER.info("No new content found")

                tracker.mark_as_processed(new_content)

            return True
        except Exception as e:
            LOGGER.error(f"Error processing new content: {e}", exc_info=True)
            return False
    
    def process_rss_items(self, new_items: Dict[str, List[Dict]]) -> bool:
        """
        Process new items found through RSS monitoring.
        
        Args:
            new_items: Dictionary of new items by content type
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not new_items or not any(new_items.values()):
                LOGGER.info("No RSS items to process")
                return True
                
            LOGGER.info(f"Processing RSS items: {sum(len(items) for items in new_items.values())} total")
            
            # Make sure we have database and API client
            if not self.db:
                LOGGER.error("Database not available")
                return False
                
            if not self.api_client:
                LOGGER.error("API client not available")
                return False
                
            with self.db:
                # For each content type, process items using API client
                for content_type, items in new_items.items():
                    for item in items:
                        item_id = item.get("id")
                        if not item_id:
                            continue
                            
                        try:
                            # Use appropriate API method based on content type
                            if content_type == "movies":
                                data = self.api_client.get_movie(item_id)
                                if data:
                                    # Convert to MovieItem
                                    item_data = self.api_client.map_api_to_item(data, "movies")
                                    # Store in database
                                    self.db.add_movie(item_data)
                            elif content_type == "shows" or content_type == "tvshows":
                                data = self.api_client.get_show(item_id)
                                if data:
                                    # Convert to ShowItem
                                    item_data = self.api_client.map_api_to_item(data, "shows")
                                    # Store in database
                                    self.db.add_show(item_data)
                            elif content_type == "episodes":
                                data = self.api_client.get_episode(item_id)
                                if data:
                                    # Convert to EpisodeItem
                                    item_data = self.api_client.map_api_to_item(data, "episodes")
                                    # Store in database
                                    self.db.add_episode(item_data)
                        except Exception as e:
                            LOGGER.error(f"Error processing {content_type} ID {item_id}: {e}")
                
                # Trigger notification for processed items
                if self.args.notify and self.NewItemTracker:
                    tracker = self.NewItemTracker(self.db)
                    new_content = tracker.get_new_content()
                    
                    if any(new_content.values()):
                        LOGGER.info(
                            f"Found new content from RSS: {', '.join(f'{k}: {len(v)}' for k, v in new_content.items() if v)}"
                        )
                        tracker.notify_new_content(new_content)
                    
                    # Mark as processed
                    tracker.mark_as_processed(new_content)
                    
                    # Also mark RSS items as processed
                    if hasattr(tracker, 'mark_rss_items_as_processed'):
                        tracker.mark_rss_items_as_processed({"rss_items": items})
                    
            return True
        except Exception as e:
            LOGGER.error(f"Error processing RSS items: {e}", exc_info=True)
            return False

    def run_sitemap_only(self) -> bool:
        """
        Run in sitemap-only mode, which only parses and updates the sitemap.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            LOGGER.info("Running in sitemap-only mode")
            
            # Make sure SitemapParser is available
            if not self.SitemapParser:
                LOGGER.error("SitemapParser not available, cannot run sitemap-only mode")
                return False
                
            # Update the sitemap if requested
            if self.args.update_sitemap:
                parser = self.SitemapParser(
                    output_file=self.args.sitemap_file or PARSED_SITEMAP_PATH,
                    error_tracker=self.error_tracker
                )
                
                # Pass modified_after if we're doing incremental updates
                if self.modified_after:
                    # Request IDs instead of full URLs if using API-first
                    result = parser.run(
                        modified_after=self.modified_after,
                        return_ids=self.args.api_first
                    )
                else:
                    result = parser.run()
                    
                if result:
                    LOGGER.info("Sitemap updated successfully")
                    # Reload the sitemap data to display the new stats
                    self.sitemap_data, self.sitemap_modified_data = self._validate_sitemap_result(result)
                    self._log_sitemap_stats()
                else:
                    LOGGER.error("Failed to update sitemap")
                    return False
            else:
                # If not updating, just display the current sitemap stats
                self._log_sitemap_stats()
                
            LOGGER.info("Sitemap-only mode completed successfully")
            return True
            
        except Exception as e:
            LOGGER.error(f"Error in sitemap-only mode: {e}", exc_info=True)
            return False

    def run_once(self) -> bool:
        """
        Run the scraper once.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Print status header
            self._print_status_header()
            
            # If in sitemap-only mode, just run that and return
            if self.args.sitemap_only:
                return self.run_sitemap_only()
                
            # Add a check for no-crawl flag that covers optimization and export scenarios
            if self.args.no_crawl and not (
                self.args.sitemap_only or 
                self.args.update_sitemap or 
                self.args.rss or 
                self.args.export
            ):
                LOGGER.info("No-crawl mode: Skipping content processing")
                return True
                
            # If export is requested with no-crawl, just do the export
            if self.args.no_crawl and self.args.export:
                from datetime import datetime  # Add this import
                LOGGER.info("Performing export in no-crawl mode")
                if self.db:
                    try:
                        # Use existing DATA_DIR for export
                        from farsiland_scraper.config import DATA_DIR
                        
                        # Create export filename with timestamp
                        export_filename = f'site_index_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
                        export_path = DATA_DIR / export_filename
                        
                        # Ensure the directory exists
                        export_path.parent.mkdir(parents=True, exist_ok=True)
                        
                        success = self.db.export_to_json(output_path=str(export_path))
                        if success:
                            LOGGER.info(f"Export completed successfully to {export_path}")
                        return success
                    except Exception as e:
                        LOGGER.error(f"Export failed: {e}")
                        return False
                else:
                    LOGGER.error("Database not initialized for export")
                    return False
            
            # If just updating sitemap without other actions, run sitemap_only
            if self.args.update_sitemap and not self.args.url and (
                self.args.sitemap_only or not hasattr(self.args, 'spiders_explicitly_set')):
                LOGGER.info("Only updating sitemap without running spiders")
                return self.run_sitemap_only()

            if self.args.update_sitemap:
                LOGGER.info("Updating sitemap data...")
                
                # Make sure SitemapParser is available
                if not self.SitemapParser:
                    LOGGER.error("SitemapParser not available, cannot update sitemap")
                    return False
                    
                parser = self.SitemapParser(
                    output_file=self.args.sitemap_file or PARSED_SITEMAP_PATH,
                    error_tracker=self.error_tracker
                )
                
                # Pass modified_after if we're doing incremental updates
                if self.modified_after:
                    # Request IDs instead of full URLs if using API-first
                    result = parser.run(
                        modified_after=self.modified_after,
                        return_ids=self.args.api_first
                    )
                else:
                    result = parser.run()
                    
                if not result:
                    LOGGER.error("Failed to update sitemap")
                    return False

                # Reload sitemap data
                self.sitemap_data, self.sitemap_modified_data = self._validate_sitemap_result(result)

            # Check RSS feeds first if enabled
            if self.args.rss and self.rss_monitor:
                LOGGER.info("Checking RSS feeds for updates...")
                new_items = self.rss_monitor.check_for_updates()
                if new_items:
                    LOGGER.info(f"Found {sum(len(items) for items in new_items.values())} items in RSS feeds")
                    self.process_rss_items(new_items)

            # Run spiders
            success = self.run_spiders()

            # Process new content if scraping was successful
            if success:
                success = self.process_new_content()

            return success
        except Exception as e:
            LOGGER.error(f"Error in run_once: {e}", exc_info=True)
            return False
            
    def _print_status_header(self):
        """Print a status header with system information."""
        try:
            # Print a divider
            LOGGER.info("=" * 50)
            LOGGER.info(f"Farsiland Scraper Run - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Print system info if psutil is available
            if PSUTIL_AVAILABLE:
                # System memory
                virtual_memory = psutil.virtual_memory()
                LOGGER.info(f"System Memory: {virtual_memory.total/(1024*1024*1024):.1f}GB total, "
                          f"{virtual_memory.available/(1024*1024*1024):.1f}GB available "
                          f"({virtual_memory.percent}% used)")
                
                # CPU info
                cpu_count = psutil.cpu_count()
                cpu_percent = psutil.cpu_percent(interval=0.1)
                LOGGER.info(f"CPU: {cpu_count} cores, {cpu_percent}% used")
                
                # Disk info
                disk = psutil.disk_usage('/')
                LOGGER.info(f"Disk: {disk.total/(1024*1024*1024):.1f}GB total, "
                          f"{disk.free/(1024*1024*1024):.1f}GB free "
                          f"({disk.percent}% used)")
                
            # Print mode and configuration
            if self.args.daemon:
                LOGGER.info(f"Running in daemon mode with interval: {SCRAPE_INTERVAL}s")
            else:
                LOGGER.info("Running in single execution mode")
                
            if self.modified_after:
                LOGGER.info(f"Incremental update mode: only processing content modified after {self.modified_after}")
                
            LOGGER.info("=" * 50)
        except Exception as e:
            LOGGER.error(f"Error printing status header: {e}")

    def run_daemon(self) -> bool:
        """
        Run the scraper in daemon mode (continuous loop).
        
        Returns:
            bool: True if exited gracefully, False otherwise
        """
        LOGGER.info(f"Starting daemon mode with interval: {SCRAPE_INTERVAL} seconds")
        
        # Use RSS polling interval if RSS is enabled
        poll_interval = self.args.rss_interval if self.args.rss else SCRAPE_INTERVAL

        try:
            while not self.interrupted:
                start_time = time.time()
                
                # Check if it's time for a full crawl
                do_full_crawl = not self.args.rss or (datetime.datetime.now().hour % 12) == 0
                
                if do_full_crawl:
                    LOGGER.info(f"Starting crawl at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    try:
                        success = self.run_once()
                        LOGGER.info(f"Crawl {'succeeded' if success else 'failed'}")
                    except Exception as e:
                        LOGGER.error(f"Error in daemon crawl cycle: {e}", exc_info=True)
                        success = False
                elif self.args.rss and self.rss_monitor:
                    # Only check RSS feeds
                    LOGGER.info("Checking RSS feeds for updates...")
                    try:
                        new_items = self.rss_monitor.check_for_updates()
                        if new_items:
                            LOGGER.info(f"Found {sum(len(items) for items in new_items.values())} items in RSS feeds")
                            self.process_rss_items(new_items)
                        else:
                            LOGGER.info("No new items in RSS feeds")
                    except Exception as e:
                        LOGGER.error(f"Error checking RSS feeds: {e}", exc_info=True)
                
                # Perform resource checks and optimization
                self.resource_monitor.check_resources(low_memory_mode=getattr(self.args, 'low_memory', False))
                
                # Calculate sleep time
                elapsed_time = time.time() - start_time
                sleep_time = max(0, poll_interval - elapsed_time)

                if self.interrupted:
                    LOGGER.info("Received interrupt signal, exiting daemon mode")
                    break

                if sleep_time > 0:
                    LOGGER.info(f"Sleeping for {sleep_time:.2f}s until next check")

                    # Break sleep into chunks to allow for quicker interruption
                    chunks = 10
                    chunk_time = sleep_time / chunks

                    for _ in range(chunks):
                        if self.interrupted:
                            break
                        time.sleep(chunk_time)
                        
                    # Perform resource checks every sleep interval
                    self.resource_monitor.check_resources(low_memory_mode=getattr(self.args, 'low_memory', False))

            return True
        except KeyboardInterrupt:
            LOGGER.info("Received keyboard interrupt. Exiting daemon mode...")
            return True
        except Exception as e:
            LOGGER.error(f"Error in daemon mode: {e}", exc_info=True)
            return False
        finally:
            # Clean up resources
            self._cleanup_resources()

    def handle_interrupt(self, signum, frame):
        """Handle interrupt signals (SIGINT, SIGTERM)."""
        LOGGER.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.interrupted = True

        if self.process:
            try:
                # Cancel any pending crawls
                self.process.crawlers.cancel()
                # Signal spiders to close
                for crawler in list(self.process.crawlers):
                    if crawler.spider:
                        crawler.spider.crawler.engine.close_spider(crawler.spider, reason='shutdown')
            except Exception as e:
                LOGGER.error(f"Error stopping crawler process: {e}")
                
    def _cleanup_resources(self):
        """Clean up resources on shutdown."""
        # Close database connection
        if self.db:
            try:
                self.db.close()
                LOGGER.info("Database connection closed")
            except Exception as e:
                LOGGER.error(f"Error closing database: {e}")
                
        # Close API client
        if self.api_client:
            if hasattr(self.api_client, 'clear_cache'):
                self.api_client.clear_cache()
            if hasattr(self.api_client, 'close'):
                self.api_client.close()
                LOGGER.info("API client closed")
                
        # Close session manager
        if self.session_manager:
            if hasattr(self.session_manager, 'close'):
                self.session_manager.close()
                LOGGER.info("Session manager closed")
                
        # Force garbage collection
        gc.collect()
        LOGGER.info("Resources cleaned up")

    def run(self) -> bool:
        """
        Run the scraper based on the specified mode.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if self.args.daemon:
                return self.run_daemon()
            else:
                return self.run_once()
        except Exception as e:
            LOGGER.error(f"Error in run: {e}", exc_info=True)
            traceback.print_exc()
            return False
        finally:
            # Clean up resources
            self._cleanup_resources()


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Farsiland Scraper',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--daemon', action='store_true', 
                           help='Run the scraper continuously')

    # Spider selection
    parser.add_argument('--spiders', nargs='+', 
                       choices=['series', 'episodes', 'movies', 'all'], 
                       default=['all'], 
                       help='Spiders to run')
    
    # URL option
    parser.add_argument('--url', type=str,
                       help='Scrape a specific URL')
    
    # Cache override
    parser.add_argument('--force-refresh', action='store_true', 
                       help='Ignore cache and re-fetch all HTML')

    # Sitemap options
    sitemap_group = parser.add_argument_group('Sitemap Options')
    sitemap_group.add_argument('--sitemap', action='store_true',
                       help='Use parsed sitemap URLs for crawling')
    sitemap_group.add_argument('--sitemap-only', action='store_true',
                       help='Only parse/update sitemap without fetching pages')
    sitemap_group.add_argument('--update-sitemap', action='store_true',
                       help='Update sitemap data before crawling')
    sitemap_group.add_argument('--sitemap-file', type=str,
                       help='Path to sitemap file')

    # Limits and throttling
    parser.add_argument('--limit', type=int,
                       help=f'Limit number of items to crawl per spider (default: {MAX_ITEMS_PER_CATEGORY})')
    parser.add_argument('--concurrent-requests', type=int,
                       help='Maximum concurrent requests')
    parser.add_argument('--download-delay', type=float,
                       help='Delay between requests in seconds')
    parser.add_argument('--max-video-size', type=int,
                       help='Maximum video size to download in bytes')

    # Output options
    output_group = parser.add_argument_group('Output Options')
    output_group.add_argument('--export', action='store_true',
                       help='Export database to JSON after scraping')
    output_group.add_argument('--export-file', type=str,
                       help='Path to export JSON file')
    output_group.add_argument('--notify', action='store_true',
                       help='Notify about new content after scraping')

    # Logging options
    logging_group = parser.add_argument_group('Logging Options')
    logging_group.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    logging_group.add_argument('--log-file', type=str,
                       help='Path to log file')
                       
    # API options
    api_group = parser.add_argument_group('API Options')
    api_source = api_group.add_mutually_exclusive_group()
    api_source.add_argument('--api-first', action='store_true', default=True,
                       help='Use API-first approach with HTML fallback (default)')
    api_source.add_argument('--html-first', action='store_false', dest='api_first',
                       help='Use HTML-first approach with API fallback')
    
    # RSS options
    rss_group = parser.add_argument_group('RSS Options')
    rss_group.add_argument('--rss', action='store_true',
                       help='Enable RSS feed monitoring')
    rss_group.add_argument('--rss-interval', type=int, default=RSS_POLL_INTERVAL,
                       help='RSS polling interval in seconds')
    rss_group.add_argument('--rss-state-file', type=str,
                       help='Path to RSS state file')
    
    # Incremental update options
    incremental_group = parser.add_argument_group('Incremental Update Options')
    date_group = incremental_group.add_mutually_exclusive_group()
    date_group.add_argument('--since', type=str,
                       help='Only process content modified since date (ISO format: YYYY-MM-DD)')
    date_group.add_argument('--modified-window', type=int,
                       help='Only process content modified in the last N days')
                       
    # Resource management options
    resource_group = parser.add_argument_group('Resource Management Options')
    resource_group.add_argument('--no-crawl', action='store_true',
                       help='Prevent automatic crawling during database operations')
    resource_group.add_argument('--low-memory', action='store_true',
                       help='Enable low memory optimization (useful for NAS/Docker)')
    resource_group.add_argument('--optimize-db', action='store_true',
                       help='Run database optimization before scraping')
    resource_group.add_argument('--checkpoint-interval', type=int, default=300,
                       help='Checkpoint interval in seconds (0 to disable)')
    resource_group.add_argument('--checkpoint-items', type=int, default=1000,
                       help='Number of items before automatic checkpoint')
                       
    # Authentication options
    auth_group = parser.add_argument_group('Authentication Options')
    auth_group.add_argument('--auth', action='store_true',
                       help='Enable authentication (uses credentials from config or env vars)')

    return parser.parse_args()


def atomic_write_json(data: Any, file_path: str) -> bool:
    """
    Write data to a JSON file using atomic file operations.
    
    Args:
        data: Data to write
        file_path: Path to write to
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Create temp file in same directory for atomic move
        fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(file_path), suffix='.json.tmp')
        
        try:
            # Write to temp file
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                # Ensure data is flushed to disk
                f.flush()
                os.fsync(f.fileno())
                
            # Atomic replace
            os.replace(temp_path, file_path)
            return True
            
        except Exception as e:
            # Clean up temp file on error
            LOGGER.error(f"Error writing file: {e}")
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            return False
            
    except Exception as e:
        LOGGER.error(f"Error in atomic_write_json: {e}")
        return False


def main() -> int:
    """
    Main entry point for the scraper.
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    try:
        # Make sure temp directory exists
        os.makedirs(TEMP_DIR, exist_ok=True)
        
        # Parse arguments
        args = parse_args()
        
        # Initialize and run scrape manager
        manager = ScrapeManager(args)
        success = manager.run()
        
        # Clean up temp directory
        if os.path.exists(TEMP_DIR):
            try:
                import shutil
                shutil.rmtree(TEMP_DIR, ignore_errors=True)
            except Exception as e:
                LOGGER.warning(f"Error cleaning up temp directory: {e}")
                
        return 0 if success else 1
    except Exception as e:
        LOGGER.error(f"Unhandled exception in main: {e}", exc_info=True)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    print("Running Farsiland Scraper...")
    exit_code = main()
    print(f"Scraper {'finished successfully' if exit_code == 0 else 'encountered an error'}.")
    sys.exit(exit_code)