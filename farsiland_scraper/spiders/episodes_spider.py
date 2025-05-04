# File: farsiland_scraper/spiders/episodes_spider.py
# Version: 8.0.0
# Last Updated: 2025-06-10

"""
Spider for scraping TV show episodes from Farsiland.

This spider:
1. Uses API-first approach with HTML fallback
2. Extracts episode metadata (title, season/episode numbers, air date, etc.)
3. Resolves and extracts video file links for each episode
4. Handles pagination and sitemap-based URL discovery
5. Integrates with error tracking system
6. Supports incremental updates and resource management

Changelog:
- [8.0.0] Consolidated HTTP libraries (standardized on aiohttp)
- [8.0.0] Improved resource management and session handling
- [8.0.0] Integrated with SessionManager for authentication
- [8.0.0] Implemented proper user-agent rotation
- [8.0.0] Added randomized rate limiting to avoid detection
- [8.0.0] Fixed unused async method and removed redundant code
- [8.0.0] Enhanced CAPTCHA handling with retries
- [8.0.0] Simplified memory management
- [8.0.0] Improved error handling and recovery
- [8.0.0] Added proper date comparison helper methods
- [8.0.0] Better handling of Scrapy event loops
- [8.0.0] Added custom settings for optimal performance
- [7.1.0] Replaced BeautifulSoup with Scrapy selectors for better performance
- [7.1.0] Added video file validation for missing or empty video files
- [7.1.0] Integrated with error tracking system
- [7.1.0] Added size limits for video downloads
- [7.0.0] Implemented API-first approach with HTML fallback
- [7.0.0] Added API client integration
- [7.0.0] Added methods to fetch episodes from API
- [7.0.0] Modified parse method to try API first
- [7.0.0] Added ID extraction from URLs
"""

import scrapy
import re
import json
import asyncio
import logging
import gc
import time
import random
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, List, Any, Set, Tuple, Union
from urllib.parse import urljoin, urlparse, urlunparse, quote, unquote
import aiohttp
from bs4 import BeautifulSoup

from farsiland_scraper.auth.session_manager import SessionManager
from farsiland_scraper.items import EpisodeItem, VideoFileItem
from farsiland_scraper.config import (
    CONTENT_ZONES,
    LOGGER,
    MAX_ITEMS_PER_CATEGORY,
    USE_SITEMAP,
    PARSED_SITEMAP_PATH,
    BASE_URL,
    API_BASE_URL,
    MAX_VIDEO_SIZE,
    IN_DOCKER
)
from farsiland_scraper.fetch import fetch_sync, CaptchaDetectedException
from farsiland_scraper.resolvers.video_link_resolver import (
    VideoLinkResolver,
    extract_quality_from_url
)

# Constants for URL validation and content extraction
EPISODE_URL_PATTERN = r"https?://[^/]+/episodes/[^/]+/?$"
CONTENT_TYPE = "episodes"
MAX_CAPTCHA_RETRIES = 3
RATE_LIMIT_DELAY_MIN = 1.5  # Minimum seconds between requests
RATE_LIMIT_DELAY_MAX = 4.0  # Maximum seconds between requests

class EpisodesSpider(scrapy.Spider):
    """
    Spider for extracting TV show episodes and their video files.
    
    This spider uses an API-first approach with HTML fallback:
    1. Try to fetch episode data from the WordPress API
    2. If API fails, fall back to HTML parsing
    3. Process video file information using aiohttp session
    4. Track errors for problematic URLs with retry mechanism
    5. Support incremental updates with proper date comparison
    6. Implement rate limiting and user-agent rotation
    7. Integrate with SessionManager for authentication
    """
    
    name = "episodes"
    allowed_domains = ["farsiland.com"]
    custom_settings = {
        'CONCURRENT_REQUESTS': 4,  # Limit concurrent requests to avoid detection
        'DOWNLOAD_DELAY': 2,       # Add delay between requests
        'RANDOMIZE_DOWNLOAD_DELAY': True,  # Randomize the delay
        'RETRY_ENABLED': True,     # Enable retries
        'RETRY_TIMES': 3,          # Number of retries
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429],  # Retry on these codes
    }
    
    def __init__(self, *args, **kwargs):
        """
        Initialize the episodes spider with improved resource management.
        
        Args:
            start_urls: Optional list of URLs to start crawling from
            max_items: Maximum number of items to crawl (default: from config)
            export_json: Whether to export results to JSON
            api_first: Whether to try API first (default: True)
            error_tracker: Optional error tracker to record issues
            modified_after: Only process content modified after this date
            checkpoint_manager: Optional checkpoint manager for resumable crawling
            low_memory: Enable low memory optimization for constrained environments
            max_video_size: Maximum video size to download
        """
        super().__init__(*args, **kwargs)
        
        # Initialize counters and limits
        self.processed_count = 0
        self.max_items = int(kwargs.get("max_items", MAX_ITEMS_PER_CATEGORY))
        self.memory_check_count = 0  # For periodic memory checks
        
        # Initialize URL sources
        self.start_urls = kwargs.get("start_urls", [])
        self.sitemap_urls = {}  # Maps URLs to lastmod timestamps
        
        # API-first setting
        self.api_first = kwargs.get("api_first", True)
        
        # Error tracker integration
        self.error_tracker = kwargs.get("error_tracker")
        
        # Checkpoint manager integration
        self.checkpoint_manager = kwargs.get("checkpoint_manager")
        
        # Low memory mode
        self.low_memory = kwargs.get("low_memory", False) or IN_DOCKER
        if self.low_memory:
            LOGGER.info("Running in low memory mode")
        
        # Size limits
        self.max_video_size = kwargs.get("max_video_size", MAX_VIDEO_SIZE)
        
        # Incremental update support
        self.modified_after = kwargs.get("modified_after")
        if self.modified_after:
            # Ensure modified_after is timezone-aware
            if self.modified_after.tzinfo is None:
                self.modified_after = self.modified_after.replace(tzinfo=timezone.utc)
            LOGGER.info(f"Only processing content modified after: {self.modified_after}")
        
        # Initialize API client if using API-first approach
        self.api_client = None
        if self.api_first:
            try:
                from farsiland_scraper.core.api_client import APIClient
                self.api_client = APIClient(base_url=API_BASE_URL)
                LOGGER.info("Using API-first approach for episodes")
            except ImportError:
                LOGGER.warning("APIClient not available, falling back to HTML-only mode")
                self.api_first = False
        else:
            LOGGER.info("Using HTML-first approach for episodes")
        
        # Load from sitemap if no start URLs provided
        if not self.start_urls:
            if USE_SITEMAP:
                self._load_sitemap_urls()
            else:
                # Default to episodes index if no sitemap or start URLs
                self.start_urls = [CONTENT_ZONES.get("episodes", f"{BASE_URL}/episodes-2025/")]
        
        # Create resolver for video links
        self.video_resolver = VideoLinkResolver()
        
        # Create session manager for authentication and rotation
        self.session_manager = SessionManager()
        
        # Initialize aiohttp session
        self.session = None
        
        # User agent rotation
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1"
        ]
        self.current_user_agent_index = 0
        
        LOGGER.info(f"EpisodesSpider initialized with max_items={self.max_items}, start_urls={len(self.start_urls)}")
        
    def _get_next_user_agent(self) -> str:
        """
        Get the next user agent in the rotation or use SessionManager's rotation.
        """
        if hasattr(self.session_manager, 'get_next_user_agent'):
            return self.session_manager.get_next_user_agent()
        else:
            agent = self.user_agents[self.current_user_agent_index]
            self.current_user_agent_index = (self.current_user_agent_index + 1) % len(self.user_agents)
            return agent
            
    async def _create_session(self):
        """
        Create aiohttp session with appropriate settings.
        Uses SessionManager if authentication is needed.
        """
        # First try to get an authenticated session from SessionManager
        try:
            self.session = await self.session_manager.ensure_session()
            LOGGER.info("Using authenticated session from SessionManager")
            return self.session
        except Exception as e:
            LOGGER.warning(f"Could not get session from SessionManager: {e}. Creating new session.")
            
        # Fall back to creating our own session if SessionManager fails
        if self.session is None or self.session.closed:
            # Configure connection limits based on environment
            connector = aiohttp.TCPConnector(
                limit=4 if self.low_memory else 10,
                force_close=True
            )
            
            # Configure timeout for different environments
            timeout = aiohttp.ClientTimeout(
                total=30,
                connect=10,
                sock_connect=10,
                sock_read=20
            )
            
            # Create session with rotating user agents
            self.session = aiohttp.ClientSession(
                connector=connector, 
                timeout=timeout,
                headers={
                    'User-Agent': self._get_next_user_agent(),
                    'Accept': 'text/html,application/json,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Range': f'bytes=0-{self.max_video_size}'
                }
            )
            
            # Establish cookies
            await self.session.get(BASE_URL)
            
        return self.session
        
    async def _close_session(self):
        """
        Close aiohttp session properly.
        Uses SessionManager's close method if the session came from there.
        """
        try:
            # Try to close through session manager first
            if hasattr(self.session_manager, 'close'):
                await self.session_manager.close()
                self.session = None
                LOGGER.info("Closed session through SessionManager")
                return
        except Exception as e:
            LOGGER.warning(f"Error closing session through SessionManager: {e}")
            
        # Fall back to closing directly if needed
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
            LOGGER.info("Directly closed session")
        
    def _load_sitemap_urls(self) -> None:
        """
        Load episode URLs from parsed sitemap file with support for incremental updates.
        
        This method populates both sitemap_urls (dictionary) and start_urls (list).
        """
        try:
            with open(PARSED_SITEMAP_PATH, 'r', encoding='utf-8') as f:
                sitemap = json.load(f)
                
                # Check if we're in incremental update mode
                if self.modified_after and "modified_episodes" in sitemap:
                    # Use only modified URLs for incremental updates
                    episode_entries = sitemap.get("modified_episodes", [])
                    if episode_entries:
                        LOGGER.info(f"Using {len(episode_entries)} modified episode URLs from sitemap")
                    else:
                        LOGGER.info("No modified episode URLs found in sitemap")
                else:
                    # Use all URLs for full scan
                    episode_entries = sitemap.get(CONTENT_TYPE, [])
                    if not episode_entries:
                        LOGGER.warning(f"No {CONTENT_TYPE} entries found in sitemap")
                        return
                    LOGGER.info(f"Using all {len(episode_entries)} episode URLs from sitemap")
                
                # Process each entry and store valid URLs
                valid_entries = []
                for entry in episode_entries:
                    if isinstance(entry, dict) and "url" in entry:
                        url = self.normalize_url(entry["url"])
                        
                        # Skip if already processed in checkpoint
                        if self.checkpoint_manager and self.checkpoint_manager.is_processed(url, CONTENT_TYPE):
                            continue
                            
                        # Skip blacklisted URLs
                        if self.error_tracker and self.error_tracker.should_skip(url):
                            LOGGER.debug(f"Skipping blacklisted URL: {url}")
                            continue
                            
                        if self._is_episode_url(url):
                            self.sitemap_urls[url] = entry.get("lastmod")
                            
                            # If entry has API ID, add it to the URL entry
                            if "api_id" in entry and entry["api_id"]:
                                self.sitemap_urls[url + "::api_id"] = entry["api_id"]
                                
                            valid_entries.append(entry)
                
                # Apply limit to the number of URLs to process
                limited_entries = valid_entries[:self.max_items]
                self.start_urls = [entry["url"] for entry in limited_entries]
                
                LOGGER.info(f"Loaded {len(self.sitemap_urls)} episode URLs from sitemap")
                LOGGER.info(f"Using first {len(self.start_urls)} URLs based on max_items={self.max_items}")
                
        except Exception as e:
            LOGGER.error(f"Failed to load sitemap data: {e}", exc_info=True)
            self.sitemap_urls = {}
    
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
            
        # Ensure URL has a scheme
        if not url.startswith(('http://', 'https://')):
            url = urljoin(BASE_URL, url)
            
        # Parse URL to components
        parsed = urlparse(url)
        
        # Rebuild URL without trailing slash
        normalized = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path.rstrip('/'),  # Remove trailing slash
            parsed.params,
            parsed.query,
            parsed.fragment
        ))
        
        return normalized
    
    def start_requests(self) -> Generator:
        """
        Generate initial requests from start URLs with error tracking.
        
        Yields:
            Scrapy Requests to episode pages
        """
        LOGGER.info(f"Starting requests for {len(self.start_urls)} episode URLs")
        
        for i, url in enumerate(self.start_urls, 1):
            if self.processed_count >= self.max_items:
                LOGGER.info(f"Reached max_items limit ({self.max_items}), stopping")
                break
                
            # Normalize URL
            url = self.normalize_url(url)
                
            # Skip blacklisted URLs
            if self.error_tracker and self.error_tracker.should_skip(url):
                LOGGER.info(f"Skipping blacklisted URL: {url}")
                continue
                
            # Skip if already processed in checkpoint
            if self.checkpoint_manager and self.checkpoint_manager.is_processed(url, CONTENT_TYPE):
                LOGGER.info(f"Skipping already processed URL: {url}")
                continue
                
            LOGGER.debug(f"Scheduling request {i}/{len(self.start_urls)}: {url}")
            
            # Extract API ID if available in sitemap data
            api_id = None
            if self.api_first and url in self.sitemap_urls:
                id_key = url + "::api_id"
                if id_key in self.sitemap_urls:
                    api_id = self.sitemap_urls[id_key]
                elif self.api_client:
                    # Try to extract ID from URL
                    api_id = self.api_client.extract_id_from_url(url)
            
            # Get lastmod from sitemap data for caching
            lastmod = self.sitemap_urls.get(url)
            
            # Pass API ID and lastmod in request meta
            meta = {
                "api_id": api_id,
                "lastmod": lastmod,
                "captcha_retries": 0  # Track CAPTCHA retry attempts
            }
            
            # Add variable delay to avoid rate limiting detection
            if i > 1:
                # Use random delay to appear more human-like
                delay = random.uniform(RATE_LIMIT_DELAY_MIN, RATE_LIMIT_DELAY_MAX)
                LOGGER.debug(f"Rate limiting delay: {delay:.2f}s")
                time.sleep(delay)
                
            yield scrapy.Request(
                url=url, 
                callback=self.parse, 
                meta=meta, 
                errback=self.handle_error,
                headers={'User-Agent': self._get_next_user_agent()}  # Rotate UA per request
            )
            
    def handle_error(self, failure):
        """
        Handle request errors and add to error tracker.
        
        Args:
            failure: Scrapy failure object
        """
        request = failure.request
        url = request.url
        
        # Extract error information
        error_type = type(failure.value).__name__
        error_msg = str(failure.value)
        
        LOGGER.error(f"Request failed for {url}: {error_type} - {error_msg}")
        
        # Track error
        if self.error_tracker:
            self.error_tracker.record_error(url, f"request_{error_type.lower()}")
            
    def parse(self, response) -> Generator:
        """
        Parse an episode page to extract metadata and video links with incremental update support.
        
        This method tries API first (if enabled), then falls back to HTML parsing.
        
        Args:
            response: Scrapy response object
            
        Yields:
            EpisodeItem with extracted data
        """
        # Rotate user agent for future requests
        response.request.headers['User-Agent'] = self._get_next_user_agent()
        
        url = self.normalize_url(response.url)
        LOGGER.info(f"Parsing episode: {url}")
        
        # Check if URL is a valid episode page
        if not self._is_episode_url(url):
            LOGGER.info(f"Skipping non-episode URL: {url}")
            # Record error for tracking
            if self.error_tracker:
                self.error_tracker.record_error(url, "invalid_episode_url")
            return
        
        # Check if URL is blacklisted
        if self.error_tracker and self.error_tracker.should_skip(url):
            LOGGER.info(f"Skipping blacklisted URL: {url}")
            return
        
        # Check if we've reached the item limit
        if self.processed_count >= self.max_items:
            LOGGER.info(f"Reached max_items limit of {self.max_items}, stopping")
            self.crawler.engine.close_spider(self, f"Reached limit of {self.max_items} items")
            return
        
        # Check for CAPTCHA
        if 'captcha' in response.text.lower() or 'recaptcha' in response.text.lower():
            captcha_retries = response.meta.get("captcha_retries", 0)
            if captcha_retries < MAX_CAPTCHA_RETRIES:
                LOGGER.warning(f"CAPTCHA detected on {url}, retrying ({captcha_retries + 1}/{MAX_CAPTCHA_RETRIES})")
                
                # Add increasing delay before retry
                retry_delay = (captcha_retries + 1) * 5  # 5, 10, 15 seconds
                time.sleep(retry_delay)
                
                # Retry with updated retry count
                meta = dict(response.meta)
                meta["captcha_retries"] = captcha_retries + 1
                
                yield scrapy.Request(
                    url=url, 
                    callback=self.parse, 
                    meta=meta,
                    errback=self.handle_error,
                    dont_filter=True
                )
                return
            else:
                LOGGER.error(f"CAPTCHA detected on {url}, max retries reached")
                # Record error for blacklisting
                if self.error_tracker:
                    self.error_tracker.record_error(url, "captcha_detected")
                return
        
        # Get API ID from response meta or try to extract from URL
        api_id = response.meta.get("api_id")
        if not api_id and self.api_first and self.api_client:
            api_id = self.api_client.extract_id_from_url(url)
        
        # Get lastmod for caching
        lastmod = response.meta.get("lastmod")
        
        # If this is from a checkpoint, check if we need to process it
        if self.checkpoint_manager and self.checkpoint_manager.is_processed(url, CONTENT_TYPE):
            LOGGER.info(f"Skipping already processed URL from checkpoint: {url}")
            return
        
        episode = None
        
        try:
            # Try API first if enabled and we have an API ID
            if self.api_first and api_id and self.api_client:
                LOGGER.info(f"Trying API for episode ID: {api_id}")
                episode = self._process_api_episode(api_id, url, lastmod)
            
            # Fall back to HTML parsing if API failed or is disabled
            if not episode:
                LOGGER.info(f"Falling back to HTML parsing for {url}")
                episode = self._process_html_episode(response, lastmod)
            
            # If we have a valid episode, increment count and yield
            if episode:
                # Check if video files are present and valid
                if not episode.get('video_files'):
                    LOGGER.warning(f"No video files found for {url} - this may be normal for some content")
                    episode['video_files'] = []
                    
                    # Record error but don't blacklist immediately
                    if self.error_tracker:
                        self.error_tracker.record_error(url, "missing_video_files")
                
                # Mark as processed in checkpoint manager
                if self.checkpoint_manager:
                    self.checkpoint_manager.mark_as_processed(url, CONTENT_TYPE)
                
                # Increment the processed count
                self.processed_count += 1
                LOGGER.info(f"Processed {self.processed_count}/{self.max_items} episodes")
                
                # Periodic memory check (every 10 items)
                self.memory_check_count += 1
                if self.memory_check_count % 10 == 0:
                    self._check_memory_usage()
                
                # Yield the episode item
                yield episode
            else:
                # Record error for blacklisting
                if self.error_tracker:
                    self.error_tracker.record_error(url, "extraction_failed")
        except Exception as e:
            LOGGER.error(f"Unexpected error processing {url}: {e}", exc_info=True)
            # Record error for blacklisting
            if self.error_tracker:
                self.error_tracker.record_error(url, "unexpected_error")
                              
    def _process_api_episode(self, episode_id, url, lastmod=None) -> Optional[EpisodeItem]:
        """
        Process an episode using the API with proper incremental update support.
        
        Args:
            episode_id: WordPress post ID
            url: Original URL for the episode
            lastmod: Last modification timestamp from sitemap
            
        Returns:
            EpisodeItem or None if API fails
        """
        try:
            # Fetch episode data from API
            episode_data = self.api_client.get_episode(episode_id, self.modified_after)
            if not episode_data:
                LOGGER.warning(f"API returned no data for episode ID: {episode_id}")
                
                # Record error for potential blacklisting
                if self.error_tracker:
                    api_error_url = f"{self.api_client.base_url}/episodes/{episode_id}"
                    self.error_tracker.record_error(api_error_url, "api_404")
                    
                return None
            
            # Check if modified date meets our criteria for incremental updates
            if self.modified_after and "modified_gmt" in episode_data:
                if not self._is_content_modified(episode_data["modified_gmt"]):
                    LOGGER.info(f"Skipping episode ID {episode_id} - not modified since {self.modified_after}")
                    return None
            
            # Map API data to episode item
            mapped_data = self.api_client.map_api_to_item(episode_data, "episodes")
            if not mapped_data:
                LOGGER.warning(f"Failed to map API data for episode ID: {episode_id}")
                return None
            
            # Create episode item
            episode = self._create_episode_item(url, lastmod)
            
            # Update with API data
            for key, value in mapped_data.items():
                if value is not None:
                    episode[key] = value
            
            # Ensure we have required fields
            if not episode.get('title'):
                episode['title'] = f"Episode {episode.get('episode_number', '?')}"
            
            if not episode.get('season_number'):
                episode['season_number'] = 1
                
            if not episode.get('episode_number'):
                episode['episode_number'] = 1
            
            # Process video files if available in API data
            if 'video_files' in mapped_data and mapped_data['video_files']:
                # API already provided video files
                pass
            else:
                # Try to extract video files from HTML as fallback
                LOGGER.info(f"API doesn't have video files, fetching HTML for {url}")
                html = fetch_sync(url, content_type=CONTENT_TYPE, lastmod=lastmod, modified_after=self.modified_after)
                if html:
                    response = scrapy.http.HtmlResponse(
                        url=url,
                        body=html.encode('utf-8')
                    )
                    # Use try/except to handle both asyncio environments
                    try:
                        # Try using existing event loop if available
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            # Create a future to run in the current loop
                            future = asyncio.ensure_future(self._extract_video_files(episode, response))
                            # Wait for the future to complete
                            loop.run_until_complete(future)
                        else:
                            # Run in the loop if it's not running
                            loop.run_until_complete(self._extract_video_files(episode, response))
                    except RuntimeError:
                        # Fallback to asyncio.run() if no loop is available or it's closed
                        asyncio.run(self._extract_video_files(episode, response))
            
            # Log the result
            self._log_extraction_result(episode)
            
            return episode
            
        except Exception as e:
            LOGGER.error(f"Error processing API episode {episode_id}: {e}", exc_info=True)
            
            # Record error for potential blacklisting
            if self.error_tracker:
                api_error_url = f"{self.api_client.base_url}/episodes/{episode_id}"
                self.error_tracker.record_error(api_error_url, "api_processing_error")
                
            return None
            
    def _process_html_episode(self, response, lastmod=None) -> Optional[EpisodeItem]:
        """
        Process an episode using HTML parsing with proper incremental update support.
        
        Args:
            response: Scrapy response object
            lastmod: Last modification timestamp from sitemap
            
        Returns:
            EpisodeItem or None if parsing fails
        """
        url = self.normalize_url(response.url)
        
        try:
            # For incremental updates, check if we need to process this page
            if self.modified_after and lastmod:
                if not self._is_content_modified(lastmod):
                    LOGGER.info(f"Skipping {url} - not modified since {self.modified_after}")
                    return None
            
            # Create episode item and extract data
            episode = self._create_episode_item(url, lastmod)
            
            # Extract basic metadata using Scrapy selectors
            self._extract_title(episode, response)
            self._extract_episode_info(episode, response, url)
            self._extract_show_info(episode, response)
            self._extract_media_info(episode, response)
            
            # Extract video files using aiohttp session
            # Use try/except to handle both asyncio environments
            try:
                # Try using existing event loop if available
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # Create a future to run in the current loop
                    future = asyncio.ensure_future(self._extract_video_files(episode, response))
                    # Wait for the future to complete
                    loop.run_until_complete(future)
                else:
                    # Run in the loop if it's not running
                    loop.run_until_complete(self._extract_video_files(episode, response))
            except RuntimeError:
                # Fallback to asyncio.run() if no loop is available or it's closed
                asyncio.run(self._extract_video_files(episode, response))
            
            # If using API-first, try to extract API ID
            if self.api_first and self.api_client:
                api_id = self.api_client.extract_id_from_url(url)
                if api_id:
                    episode['api_id'] = api_id
                    episode['api_source'] = 'html_extraction'
            
            # Log the result
            self._log_extraction_result(episode)
            
            return episode
            
        except Exception as e:
            LOGGER.error(f"Error parsing HTML episode {url}: {e}", exc_info=True)
            
            # Record error for potential blacklisting
            if self.error_tracker:
                self.error_tracker.record_error(url, "html_parsing_error")
                
            return None
    
    def _is_content_modified(self, date_str: str) -> bool:
        """
        Check if content has been modified since the modified_after date.
        
        Args:
            date_str: Date string in ISO format
            
        Returns:
            True if content has been modified, False otherwise
        """
        if not self.modified_after:
            return True
            
        try:
            # Convert to datetime objects for comparison
            modified_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            
            # Ensure timezone awareness
            if modified_date.tzinfo is None:
                modified_date = modified_date.replace(tzinfo=timezone.utc)
                
            # Return True if modified after the cutoff date
            return modified_date > self.modified_after
            
        except (ValueError, TypeError, AttributeError) as e:
            LOGGER.warning(f"Error comparing modification dates: {e}")
            # Continue processing to be safe if any error occurs
            return True
    
    def _create_episode_item(self, url: str, lastmod: Optional[str] = None) -> EpisodeItem:
        """
        Create a new EpisodeItem with initial values.
        
        Args:
            url: The episode URL
            lastmod: Last modification timestamp from sitemap
            
        Returns:
            Initialized EpisodeItem
        """
        return EpisodeItem(
            url=url,
            sitemap_url=url,
            lastmod=lastmod,
            is_new=True,
            video_files=[],
            last_scraped=datetime.now().isoformat()
        )
        
    def _extract_title(self, episode: EpisodeItem, response) -> None:
        """
        Extract the episode title from the HTML.
        
        Args:
            episode: Episode item to update
            response: Scrapy response object
        """
        try:
            # Try different selectors for the title using Scrapy selectors
            title_selectors = [
                ".player-title::text", 
                "h1::text", 
                ".episodiotitle h3::text", 
                "meta[property='og:title']::attr(content)"
            ]
            
            for selector in title_selectors:
                title = response.css(selector).get()
                if title:
                    episode['title'] = title.strip()
                    break
            
            # Fallback if no title found
            if not episode.get('title'):
                episode['title'] = "Unknown Episode"
                LOGGER.warning(f"Could not extract title for {episode['url']}")
                
        except Exception as e:
            LOGGER.warning(f"Error extracting title: {e}")
            episode['title'] = "Unknown Episode"
    
    def _extract_episode_info(self, episode: EpisodeItem, response, url: str) -> None:
        """
        Extract season and episode numbers from the HTML.
        
        Args:
            episode: Episode item to update
            response: Scrapy response object
            url: The episode URL for fallback extraction
        """
        try:
            # Default values
            season_number = 1
            episode_number = 1
            
            # Try to extract from the 'numerando' element (format: "1 - 2")
            numerando = response.css(".numerando::text").get()
            if numerando:
                parts = numerando.strip().split("-")
                if len(parts) == 2:
                    try:
                        season_number = int(parts[0].strip())
                        episode_number = int(parts[1].strip())
                    except ValueError:
                        LOGGER.debug(f"Could not parse numerando: {numerando}")
            
            # Fallback: Try to extract from breadcrumbs
            if season_number == 1:
                breadcrumb_items = response.css(".breadcrumb li::text").getall()
                for text in breadcrumb_items:
                    if "season" in text.lower():
                        match = re.search(r'season\s*(\d+)', text.lower())
                        if match:
                            try:
                                season_number = int(match.group(1))
                            except ValueError:
                                pass
            
            # Fallback: Try to extract from URL
            if episode_number == 1:
                match = re.search(r'ep(?:isode)?[_-]?(\d+)', url.lower())
                if match:
                    try:
                        episode_number = int(match.group(1))
                    except ValueError:
                        pass
            
            # Fallback: Try to extract from title
            if episode_number == 1 and episode.get('title'):
                match = re.search(r'episode\s*(\d+)', episode['title'].lower())
                if match:
                    try:
                        episode_number = int(match.group(1))
                    except ValueError:
                        pass
            
            episode['season_number'] = season_number
            episode['episode_number'] = episode_number
            
        except Exception as e:
            LOGGER.warning(f"Error extracting episode info: {e}")
            episode['season_number'] = 1
            episode['episode_number'] = 1
           
    def _extract_show_info(self, episode: EpisodeItem, response) -> None:
        """
        Extract the parent show URL and related information.
        
        Args:
            episode: Episode item to update
            response: Scrapy response object
        """
        try:
            # Try different selectors for show link
            show_link_selectors = [
                ".breadcrumb li:nth-last-child(2) a::attr(href)",
                "div.pag_episodes a[href*='/tvshows/']::attr(href)",
                "a[href*='/tvshows/']::attr(href)",
                "a[href*='/series/']::attr(href)"
            ]
            
            for selector in show_link_selectors:
                show_url = response.css(selector).get()
                if show_url:
                    show_url = show_url.rstrip("/")
                    if '/tvshows/' in show_url or '/series/' in show_url:
                        episode['show_url'] = show_url
                        
                        # Try to extract show_id if using API-first
                        if self.api_first and self.api_client and not episode.get('show_id'):
                            show_id = self.api_client.extract_id_from_url(show_url)
                            if show_id:
                                episode['show_id'] = show_id
                                
                        break
            
            # Fallback if no show URL found
            if not episode.get('show_url'):
                LOGGER.warning(f"Could not extract show URL for {episode['url']}")
                # Create a fallback URL based on episode URL
                base_url = episode['url'].split('/episodes/')[0]
                episode_slug = episode['url'].split('/episodes/')[1]
                show_slug = episode_slug.split('-')[0] if '-' in episode_slug else episode_slug
                episode['show_url'] = f"{base_url}/tvshows/{show_slug}"
                
        except Exception as e:
            LOGGER.warning(f"Error extracting show info: {e}")
            episode['show_url'] = None
    
    def _extract_media_info(self, episode: EpisodeItem, response) -> None:
        """
        Extract media information (thumbnail, air date).
        
        Args:
            episode: Episode item to update
            response: Scrapy response object
        """
        try:
            # Extract thumbnail
            thumbnail_selectors = [
                "meta[property='og:image']::attr(content)",
                ".poster img::attr(src)",
                ".thumb img::attr(src)",
                ".thumb img::attr(data-src)"
            ]
            
            for selector in thumbnail_selectors:
                thumb = response.css(selector).get()
                if thumb:
                    episode['thumbnail'] = thumb
                    break
            
            # Extract air date
            date_selectors = [
                ".extra span.date + span.date::text",
                ".episodiotitle .date::text",
                ".date[itemprop='dateCreated']::text",
                "span.date::text"
            ]
            
            for selector in date_selectors:
                date = response.css(selector).get()
                if date:
                    episode['air_date'] = date.strip()
                    break
            
        except Exception as e:
            LOGGER.warning(f"Error extracting media info: {e}")
            
    async def _extract_video_files(self, episode: EpisodeItem, response) -> None:
        """
        Extract video file information from the HTML with improved error handling.
        
        Args:
            episode: Episode item to update
            response: Scrapy response object
        """
        try:
            # Find file entries in download table
            file_entries = []
            
            # Look for fileids in download table rows using Scrapy selectors
            form_fileids = []
            for fileid in response.css("#download table tr[id^='link-'] input[name='fileid']::attr(value)").getall():
                row_selector = f"#download table tr form input[name='fileid'][value='{fileid}']"
                row = response.css(row_selector).xpath("./ancestor::tr[1]")
                
                quality = row.css("strong.quality::text").get() or row.css("td:nth-child(2)::text").get() or "unknown"
                size = row.css("td:nth-child(3)::text").get() or ""
                
                file_entries.append({
                    "fileid": fileid,
                    "quality": quality.strip(),
                    "size": size.strip()
                })
            
            # If no file entries found in table, look for forms
            if not file_entries:
                for form_id in response.css("form[id^='dlform']::attr(id)").getall():
                    fileid = response.css(f"form#{form_id} input[name='fileid']::attr(value)").get()
                    if fileid:
                        file_entries.append({
                            "fileid": fileid,
                            "quality": "unknown",
                            "size": ""
                        })
            
            # If file entries found, process them with the resolver
            if file_entries:
                LOGGER.debug(f"Found {len(file_entries)} file entries to resolve")
                
                # Process the entries using the resolver
                session = await self._create_session()
                
                # Update session headers to handle size limits if needed
                if not 'Range' in session.headers:
                    session.headers.update({
                        'Range': f'bytes=0-{self.max_video_size}'
                    })
                
                video_files = await self._resolve_links(file_entries, session)
                episode['video_files'] = video_files
            
            # If no video files found or resolved, look for direct MP4 links
            if not episode.get('video_files') or not episode['video_files']:
                for href in response.css("a[href$='.mp4']::attr(href)").getall():
                    if href:
                        quality = extract_quality_from_url(href)
                        episode['video_files'].append({
                            "quality": quality,
                            "url": href,
                            "mirror_url": None,
                            "size": ""
                        })
                if episode.get('video_files') and episode['video_files']:
                    LOGGER.info(f"Found {len(episode['video_files'])} direct MP4 links")
                else:
                    LOGGER.warning(f"No video files found for {episode['url']}")
                    
                    # Add empty list to avoid errors elsewhere
                    episode['video_files'] = []
                    
                    # Record error for potential blacklisting
                    if self.error_tracker:
                        self.error_tracker.record_error(episode['url'], "no_video_files")
                    
        except Exception as e:
            LOGGER.error(f"Error extracting video files: {e}", exc_info=True)
            episode['video_files'] = []
    
    async def _resolve_links(self, file_entries: List[Dict[str, str]], session: aiohttp.ClientSession = None) -> List[Dict[str, str]]:
        """
        Resolve download links for the file entries with improved memory management.
        
        Args:
            file_entries: List of file entries with fileids
            session: Optional aiohttp session to use
            
        Returns:
            List of video file dictionaries
        """
        video_files = []
        
        try:
            # Create session if not provided
            if not session:
                session = await self._create_session()
            
            # Process each file entry
            for entry in file_entries:
                try:
                    fileid = entry.get('fileid')
                    if not fileid:
                        continue
                    
                    LOGGER.debug(f"Resolving fileid: {fileid}")
                    
                    # Use the VideoLinkResolver
                    links = await self.video_resolver.get_video_links(session, fileid)
                    
                    if links:
                        for link in links:
                            video_files.append({
                                "quality": entry.get("quality", link.get("quality", "unknown")),
                                "url": link["url"],
                                "mirror_url": link.get("mirror_url"),
                                "size": entry.get("size", "")
                            })
                    else:
                        LOGGER.warning(f"No links resolved for fileid {fileid}")
                        
                        # Try fallback with direct POST request
                        try:
                            # Prepare the form data for submission
                            form_data = {"fileid": fileid}
                            
                            # Send POST request
                            response = await session.post(
                                f"{BASE_URL}/get/",
                                data=form_data,
                                allow_redirects=True,
                                timeout=30
                            )
                            
                            # Check for successful response
                            if response.status == 200:
                                # Parse HTML with BeautifulSoup
                                html_text = await response.text()
                                soup = BeautifulSoup(html_text, "html.parser")
                                
                                # Look for direct MP4 links
                                mp4_links = []
                                for a in soup.select("a[href$='.mp4']"):
                                    href = a.get("href")
                                    if href:
                                        mp4_links.append(href)
                                
                                # If MP4 links found, use them
                                if mp4_links:
                                    for mp4_url in mp4_links:
                                        quality = extract_quality_from_url(mp4_url)
                                        video_files.append({
                                            "quality": entry.get("quality", quality),
                                            "url": mp4_url,
                                            "mirror_url": None,
                                            "size": entry.get("size", "")
                                        })
                                else:
                                    LOGGER.warning(f"No MP4 links found for fileid {fileid}")
                            else:
                                LOGGER.warning(f"Failed to resolve fileid {fileid}: HTTP {response.status}")
                                
                        except Exception as e:
                            LOGGER.warning(f"Fallback resolution failed for fileid {fileid}: {e}")
                        
                except Exception as e:
                    LOGGER.warning(f"Failed to resolve fileid={entry.get('fileid')}: {e}")
                    
            return video_files
                
        except Exception as e:
            LOGGER.error(f"Error in link resolution: {e}", exc_info=True)
            return video_files
    
    def _log_extraction_result(self, episode: EpisodeItem) -> None:
        """
        Log the result of the extraction process.
        
        Args:
            episode: The extracted episode item
        """
        title = episode.get('title', 'Unknown')
        season = episode.get('season_number', 0)
        ep_num = episode.get('episode_number', 0)
        video_count = len(episode.get('video_files', []))
        
        source = "API" if episode.get('api_id') else "HTML"
        LOGGER.info(f"Extracted ({source}): S{season}E{ep_num} - {title} ({video_count} video files)")
    
    def _is_episode_url(self, url: str) -> bool:
        """
        Check if a URL is a valid episode page.
        
        Args:
            url: URL to check
            
        Returns:
            True if the URL is a valid episode page
        """
        if not url:
            return False
            
        # Use regular expression to validate URL format
        return bool(re.match(EPISODE_URL_PATTERN, url))
        
    def _check_memory_usage(self) -> None:
        """
        Check and optimize memory usage during crawling.
        Important for Docker/NAS environments with limited resources.
        """
        try:
            # Only import psutil if needed
            import psutil
            process = psutil.Process()
            
            # Get memory info
            memory_info = process.memory_info()
            memory_percent = process.memory_percent()
            
            # Log current memory usage
            LOGGER.debug(f"Memory usage: {memory_info.rss / (1024*1024):.1f}MB ({memory_percent:.1f}%)")
            
            # If memory usage is high, take action
            if memory_percent > 70:  # Threshold for action
                LOGGER.warning(f"High memory usage detected: {memory_percent:.1f}%")
                
                # Force garbage collection
                gc.collect()
                
                # Clear API client cache if available
                if self.api_client and hasattr(self.api_client, '_optimize_cache_size'):
                    self.api_client._optimize_cache_size()
                    
                # Log memory after optimization
                memory_after = process.memory_percent()
                LOGGER.info(f"Memory after optimization: {memory_after:.1f}% (reduced by {memory_percent - memory_after:.1f}%)")
                
        except ImportError:
            # psutil not available
            pass
        except Exception as e:
            LOGGER.warning(f"Error checking memory usage: {e}")
    
    async def close_spider(self, reason):
        """
        Clean up resources when spider closes.
        
        Args:
            reason: Reason for spider closure
        """
        LOGGER.info(f"Spider closing asynchronously: {reason}")
        
        # Save checkpoint if available
        if self.checkpoint_manager:
            self.checkpoint_manager.save_checkpoint(force=True)
            
        # Clean up API client resources
        if self.api_client:
            if hasattr(self.api_client, 'clear_cache'):
                self.api_client.clear_cache()
            if hasattr(self.api_client, 'close'):
                self.api_client.close()
        
        # Close aiohttp session asynchronously
        await self._close_session()
        
        # Force garbage collection to free memory
        gc.collect()
        
        # Call the synchronous closed method for any additional cleanup
        self.closed(reason)
    
    def closed(self, reason):
        """
        Additional synchronous cleanup when spider closes.
        
        Args:
            reason: Reason for spider closure
        """
        LOGGER.info(f"Spider closed synchronously: {reason}")
        
        # Perform any additional synchronous cleanup here
        # This is called by Scrapy after the spider is closed
        
        # Final garbage collection
        gc.collect()