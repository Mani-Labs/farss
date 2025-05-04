# File: farsiland_scraper/spiders/series_spider.py
# Version: 8.0.4
# Last Updated: 2025-06-03

"""
Spider for scraping TV show/series metadata from Farsiland.

This spider:
1. Uses API-first approach with HTML fallback
2. Extracts series metadata (title, poster, season count, episode links, etc.)
3. Discovers episode URLs but does not process them (left to episodes_spider.py)
4. Handles pagination and sitemap-based URL discovery
5. **Includes crawling of older series index pages not covered by the sitemap.**
6. Supports incremental updates with proper date filtering
7. Standardizes URL normalization for consistent handling
8. Implements memory optimizations for Docker/NAS environments
9. Provides proper resource cleanup

Changelog:
- [8.0.4] Added crawling of '/iranian-series/' index page to discover older series not in sitemap.
- [8.0.3] Corrected setting name check for force refresh in start_requests to 'FORCE_REFRESH'.
- [8.0.2] Modified start_requests to respect FORCE_RELOAD flag and bypass checkpoint processed check.
- [8.0.1] Updated HTML extraction selectors based on user feedback for improved parsing.
- [8.0.0] Added proper incremental update support
- [8.0.0] Standardized URL normalization for consistency
- [8.0.0] Improved memory efficiency for large datasets
- [8.0.0] Added resource cleanup on spider close
- [8.0.0] Enhanced error handling with better recovery
- [8.0.0] Added support for non-ASCII URLs and Persian text
- [8.0.0] Optimized for Docker/headless environments
- [7.1.0] Replaced BeautifulSoup with Scrapy selectors for better performance
- [7.1.0] Added error tracking integration
- [7.1.0] Fixed unicode handling in logs
- [7.1.0] Added validation for missing data fields
"""

import scrapy
import re
import json
import logging
import gc
import unicodedata
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List, Set
from urllib.parse import urljoin, urlparse, urlunparse, quote, unquote

from farsiland_scraper.items import ShowItem
from farsiland_scraper.config import (
    CONTENT_ZONES,
    LOGGER,
    MAX_ITEMS_PER_CATEGORY,
    USE_SITEMAP,
    PARSED_SITEMAP_PATH,
    BASE_URL,
    API_BASE_URL,
    IN_DOCKER
)
from farsiland_scraper.fetch import fetch_sync
from farsiland_scraper.core.api_client import APIClient
from farsiland_scraper.utils.error_tracker import ErrorTracker

# Constants for URL validation and content extraction
SERIES_URL_PATTERN = r"https?://[^/]+/tvshows/[^/]+/?$"
CONTENT_TYPE = "shows"  # The database table is "shows" even though the spider is named "series"

class SeriesSpider(scrapy.Spider):
    """
    Spider for extracting TV series metadata and related episode URLs.

    This spider uses an API-first approach with HTML fallback:
    1. Try to fetch series data from the WordPress API
    2. If API fails, fall back to HTML parsing
    3. Maintain backward compatibility with HTML-only approach
    4. Properly handle incremental updates
    """

    name = "series"
    allowed_domains = ["farsiland.com"]

    def __init__(self, *args, **kwargs):
        """
        Initialize the series spider.

        Args:
            start_urls: Optional list of URLs to start crawling from
            max_items: Maximum number of items to crawl (default: from config)
            export_json: Whether to export results to JSON
            api_first: Whether to try API first (default: True)
            error_tracker: Error tracker instance (optional)
            modified_after: Only process content modified after this date
            checkpoint_manager: Optional checkpoint manager for resumable crawling
            low_memory: Whether to enable low memory optimizations
        """
        super().__init__(*args, **kwargs)

        # Initialize counters and limits
        self.processed_count = 0
        self.max_items = int(kwargs.get("max_items", MAX_ITEMS_PER_CATEGORY))

        # Initialize URL sources
        self.start_urls = kwargs.get("start_urls", [])
        self.sitemap_urls = {}  # Maps URLs to lastmod timestamps

        # API-first setting
        self.api_first = kwargs.get("api_first", True)

        # Error tracking
        self.error_tracker = kwargs.get("error_tracker")
        if not self.error_tracker:
            try:
                self.error_tracker = ErrorTracker()
                LOGGER.info("Error tracker initialized")
            except Exception as e:
                LOGGER.warning(f"Could not initialize error tracker: {e}")
                self.error_tracker = None

        # Incremental update support
        self.modified_after = kwargs.get("modified_after")
        if self.modified_after:
            LOGGER.info(f"Spider will only process content modified after {self.modified_after}")

        # Checkpoint manager
        self.checkpoint_manager = kwargs.get("checkpoint_manager")

        # Low memory mode for Docker/NAS environments
        self.low_memory = kwargs.get("low_memory", IN_DOCKER)
        if self.low_memory:
            LOGGER.info("Running in low memory mode")

        # Initialize API client if using API-first approach
        if self.api_first:
            try:
                self.api_client = APIClient(base_url=API_BASE_URL)
                LOGGER.info("Using API-first approach")
            except Exception as e:
                LOGGER.error(f"Failed to initialize API client: {e}")
                self.api_client = None
                self.api_first = False
        else:
            self.api_client = None
            LOGGER.info("Using HTML-first approach")

        # Load from sitemap if no start URLs provided, otherwise use provided start_urls
        if not self.start_urls and USE_SITEMAP:
            self._load_sitemap_urls()
        elif not self.start_urls:
            # Default to series index if no sitemap or start URLs
            self.start_urls = [CONTENT_ZONES.get("series", f"{BASE_URL}/series-22/")]

        # Add the older series index page as an additional starting point
        # This ensures we crawl older series not present in the sitemap
        self.start_urls.append(f"{BASE_URL}/iranian-series/")
        # Add the /tvshows/ index page as well, as some older series might be there
        self.start_urls.append(f"{BASE_URL}/tvshows/")


        # Remove duplicates from start_urls in case they were added multiple times
        self.start_urls = list(dict.fromkeys(self.start_urls))


        LOGGER.info(f"SeriesSpider initialized with max_items={self.max_items}, start_urls={len(self.start_urls)}")

    def normalize_text(self, text: str) -> str:
        """
        Normalize Unicode text, especially for Persian/Arabic characters.

        Args:
            text: Text to normalize

        Returns:
            Normalized text
        """
        if not text:
            return ""

        # Convert to NFKC form (compatibility decomposition + canonical composition)
        text = unicodedata.normalize('NFKC', text)

        # Convert Persian/Arabic digits to Latin
        persian_digits = {
            '۰': '0', '۱': '1', '۲': '2', '۳': '3', '۴': '4',
            '۵': '5', '۶': '6', '۷': '7', '۸': '8', '۹': '9'
        }
        for persian, latin in persian_digits.items():
            text = text.replace(persian, latin)

        return text

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
        try:
            parsed = urlparse(url)

            # Normalize path - ensure proper encoding of non-ASCII characters
            path = unquote(parsed.path)
            path = self.normalize_text(path)

            # Reconstruct URL with proper encoding
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
            # If URL parsing fails, just remove trailing slash
            return url.rstrip('/')

    def _load_sitemap_urls(self) -> None:
        """
        Load series URLs from parsed sitemap file.

        This method populates both sitemap_urls (dictionary) and start_urls (list).
        """
        try:
            with open(PARSED_SITEMAP_PATH, 'r', encoding='utf-8') as f:
                sitemap = json.load(f)

            # Look for series entries in the sitemap (could be under different keys)
            # Check for modified series first if we're doing incremental updates
            if self.modified_after and "modified_shows" in sitemap:
                series_entries = sitemap.get("modified_shows", [])
                LOGGER.info(f"Using modified shows from sitemap for incremental update")
            elif self.modified_after and "modified_series" in sitemap:
                series_entries = sitemap.get("modified_series", [])
                LOGGER.info(f"Using modified series from sitemap for incremental update")
            else:
                # Not an incremental update or no modified entries, use all entries
                series_entries = (
                    sitemap.get(CONTENT_TYPE, []) or
                    sitemap.get("series", []) or
                    sitemap.get("tvshows", []) or
                    sitemap.get("shows", [])  # Last fallback
                )

            if not series_entries:
                LOGGER.warning(f"No series entries found in sitemap")
                return

            LOGGER.info(f"Found {len(series_entries)} series entries in sitemap")

            # Process each entry and store valid URLs
            valid_entries = []
            for entry in series_entries:
                if isinstance(entry, dict) and "url" in entry:
                    url = self.normalize_url(entry["url"])

                    # Skip blacklisted URLs
                    if self.error_tracker and self.error_tracker.should_skip(url):
                        LOGGER.debug(f"Skipping blacklisted URL: {url}")
                        continue

                    if self._is_series_url(url):
                        self.sitemap_urls[url] = entry.get("lastmod")

                        # If entry has API ID, add it to the URL entry
                        if "api_id" in entry and entry["api_id"]:
                            self.sitemap_urls[url + "::api_id"] = entry["api_id"]

                        valid_entries.append(entry)

            # Apply limit to the number of URLs to process
            limited_entries = valid_entries[:self.max_items]
            self.start_urls = [entry["url"] for entry in limited_entries]

            LOGGER.info(f"Loaded {len(self.sitemap_urls)} series URLs from sitemap")
            LOGGER.info(f"Using first {len(self.start_urls)} URLs based on max_items={self.max_items}")

            # Free memory in low memory mode
            if self.low_memory:
                del valid_entries
                gc.collect()

        except Exception as e:
            LOGGER.error(f"Failed to load sitemap data: {e}", exc_info=True)

    def start_requests(self) -> Generator:
        """
        Generate initial requests from start URLs.
        Includes URLs from sitemap and additional index pages.

        Yields:
            Scrapy Requests to series pages or index pages
        """
        LOGGER.info(f"Starting requests for {len(self.start_urls)} series URLs")

        # Check for force refresh setting - Using 'FORCE_REFRESH' based on log output
        force_refresh = self.crawler.settings.getbool('FORCE_REFRESH', False)
        if force_refresh:
            LOGGER.info("Force refresh enabled, ignoring checkpoint processed list.")


        for i, url in enumerate(self.start_urls, 1):
            if self.processed_count >= self.max_items:
                LOGGER.info(f"Reached max_items limit ({self.max_items}), stopping")
                break

            # Skip blacklisted URLs
            if self.error_tracker and self.error_tracker.should_skip(url):
                LOGGER.info(f"Skipping blacklisted URL: {url}")
                continue

            # Normalize URL for consistency
            normalized_url = self.normalize_url(url)

            # Log progress, but limit verbosity
            if i % 100 == 0 or i <= 5:
                LOGGER.debug(f"Scheduling request {i}/{len(self.start_urls)}: {normalized_url}")

            # Determine the correct callback based on the URL
            if "/iranian-series/" in normalized_url or "/tvshows/" in normalized_url:
                 # These are likely index pages for older series
                 callback = self.parse_old_series_index
                 LOGGER.debug(f"Scheduling index page request: {normalized_url}")
            else:
                 # These are likely direct series pages (from sitemap or default)
                 callback = self.parse
                 LOGGER.debug(f"Scheduling series page request: {normalized_url}")


            # Extract API ID if available in sitemap data for direct series pages
            api_id = None
            if callback == self.parse and normalized_url in self.sitemap_urls:
                id_key = normalized_url + "::api_id"
                if id_key in self.sitemap_urls:
                    api_id = self.sitemap_urls[id_key]
                else:
                    # Try to extract ID from URL
                    if self.api_client:
                        api_id = self.api_client.extract_id_from_url(normalized_url)

            # Pass API ID in request meta if available
            meta = {"api_id": api_id} if api_id else {}

            # Add lastmod timestamp to meta for incremental updates (primarily for sitemap URLs)
            if normalized_url in self.sitemap_urls and self.sitemap_urls[normalized_url]:
                meta["lastmod"] = self.sitemap_urls[normalized_url]

            # Checkpoint support - check if URL has already been processed
            # Only skip if force refresh is NOT enabled
            # Note: We apply checkpoint check to series pages (callback=self.parse),
            # but generally not to index pages (callback=self.parse_old_series_index)
            # as we want to re-crawl index pages to find new series.
            if callback == self.parse and self.checkpoint_manager and not force_refresh and self.checkpoint_manager.is_processed(normalized_url, "shows"):
                LOGGER.debug(f"Skipping already processed URL due to checkpoint: {normalized_url}")
                continue

            # Add to pending in checkpoint manager (only for series pages)
            if callback == self.parse and self.checkpoint_manager:
                self.checkpoint_manager.add_pending(normalized_url, "shows")

            yield scrapy.Request(url=normalized_url, callback=callback, meta=meta)

    def parse_old_series_index(self, response) -> Generator:
        """
        Parse index pages like /iranian-series/ to extract links to individual series pages.

        Args:
            response: Scrapy response object

        Yields:
            Scrapy Requests to individual series pages
        """
        LOGGER.info(f"Parsing old series index page: {response.url}")

        # Find links to individual series pages.
        # Need to inspect the HTML of /iranian-series/ and /tvshows/ to find the correct selectors.
        # Assuming links are within <a> tags and point to /tvshows/ or /series-XX/ URLs.
        # This is a common pattern, but might need adjustment based on actual HTML.
        series_link_selectors = [
            "a[href*='/tvshows/']::attr(href)",
            "a[href*='/series-']::attr(href)",
            ".items article a::attr(href)", # Common selector for item links
            ".posts-list a::attr(href)" # Another common selector
        ]

        for selector in series_link_selectors:
            for href in response.css(selector).getall():
                series_url = self.normalize_url(urljoin(response.url, href))

                # Ensure the extracted URL is a valid series URL and not an episode or other page
                if self._is_series_url(series_url):
                    LOGGER.debug(f"Discovered series URL from index: {series_url}")

                    # Check if URL is blacklisted
                    if self.error_tracker and self.error_tracker.should_skip(series_url):
                        LOGGER.info(f"Skipping blacklisted URL from index: {series_url}")
                        continue

                    # Checkpoint support - check if URL has already been processed
                    # We still check checkpoint here to avoid re-crawling already processed series pages
                    force_refresh = self.crawler.settings.getbool('FORCE_REFRESH', False)
                    if self.checkpoint_manager and not force_refresh and self.checkpoint_manager.is_processed(series_url, "shows"):
                         LOGGER.debug(f"Skipping already processed URL from index due to checkpoint: {series_url}")
                         continue

                    # Add to pending in checkpoint manager
                    if self.checkpoint_manager:
                        self.checkpoint_manager.add_pending(series_url, "shows")

                    # Yield a request to parse the individual series page
                    yield scrapy.Request(url=series_url, callback=self.parse)

        # Implement pagination if the index page has multiple pages
        # Need to inspect the HTML for pagination links.
        # Common selectors for "Next Page" links:
        next_page_selectors = [
            "a.next::attr(href)",
            "a[rel='next']::attr(href)",
            ".pagination a.page-numbers::attr(href)",
            "a[aria-label='Next page']::attr(href)"
        ]

        for selector in next_page_selectors:
            next_page_link = response.css(selector).get()
            if next_page_link:
                next_page_url = self.normalize_url(urljoin(response.url, next_page_link))
                # Avoid crawling the same page again or getting stuck in a loop
                if next_page_url != self.normalize_url(response.url):
                     LOGGER.info(f"Following pagination link: {next_page_url}")
                     yield scrapy.Request(url=next_page_url, callback=self.parse_old_series_index)
                     break # Assume only one next page link per page


    def parse(self, response) -> Generator:
        """
        Parse a series page to extract metadata.

        This method tries API first (if enabled), then falls back to HTML parsing.

        Args:
            response: Scrapy response object

        Yields:
            ShowItem with extracted data
        """
        url = self.normalize_url(response.url)
        LOGGER.info(f"Parsing series: {url}")

        # Check if URL is a valid series page
        if not self._is_series_url(url):
            LOGGER.info(f"Skipping non-series URL: {url}")
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

        # Get API ID from response meta or try to extract from URL
        api_id = response.meta.get("api_id")
        if not api_id and self.api_first and self.api_client:
            api_id = self.api_client.extract_id_from_url(url)

        # Get lastmod from response meta
        lastmod = response.meta.get("lastmod")

        show = None

        # Try API first if enabled and we have an API ID
        if self.api_first and api_id and self.api_client:
            LOGGER.info(f"Trying API for series ID: {api_id}")
            show = self._process_api_series(api_id, url, lastmod)

        # Fall back to HTML parsing if API failed or is disabled
        if not show:
            LOGGER.info(f"Falling back to HTML parsing for {url}")
            try:
                show = self._process_html_series(response, lastmod)
            except Exception as e:
                LOGGER.error(f"Error parsing HTML series {url}: {e}", exc_info=True)
                # Record error for potential blacklisting
                if self.error_tracker:
                    self.error_tracker.record_error(url, "html_parsing_error")
                return

        # If we have a valid show, increment count and yield
        if show:
            # Check if this is a modified item we should process
            if self.modified_after and "modified_gmt" in show:
                try:
                    # Parse modified date from show
                    modified_date = None
                    if show["modified_gmt"]:
                        modified_date = datetime.fromisoformat(show["modified_gmt"].replace('Z', '+00:00'))

                    # Parse lastmod date as fallback
                    if not modified_date and lastmod:
                        modified_date = datetime.fromisoformat(lastmod.replace('Z', '+00:00'))

                    # If we have both a modified date and filter date, compare them
                    if modified_date and self.modified_after:
                        # Make modified_after timezone-aware if it isn't already
                        filter_date = self.modified_after
                        if filter_date.tzinfo is None:
                            filter_date = filter_date.replace(tzinfo=timezone.utc)

                        # Skip if this item is older than our filter date
                        if modified_date <= filter_date:
                            LOGGER.info(f"Skipping unmodified item: {url} (modified: {modified_date}, filter: {filter_date})")
                            return

                        # Mark as modified for downstream processing
                        show['is_modified'] = True
                except Exception as e:
                    LOGGER.warning(f"Error checking modification date: {e}")

            # Increment the processed count
            self.processed_count += 1
            LOGGER.info(f"Processed {self.processed_count}/{self.max_items} series")

            # Mark as processed in checkpoint manager
            if self.checkpoint_manager:
                self.checkpoint_manager.mark_as_processed(url, "shows")
                # Periodic checkpoint saving
                if self.processed_count % 100 == 0:
                    self.checkpoint_manager.save_checkpoint()

            # Yield the show item
            yield show

    def _process_api_series(self, series_id, url, lastmod=None) -> Optional[ShowItem]:
        """
        Process a series using the API.

        Args:
            series_id: WordPress post ID
            url: Original URL for the series
            lastmod: Last modification timestamp from sitemap

        Returns:
            ShowItem or None if API fails
        """
        try:
            # Fetch series data from API
            series_data = self.api_client.get_show(series_id)
            if not series_data:
                LOGGER.warning(f"API returned no data for series ID: {series_id}")
                return None

            # Map API data to show item
            mapped_data = self.api_client.map_api_to_item(series_data, "shows", self.modified_after)
            if not mapped_data:
                LOGGER.warning(f"Failed to map API data for series ID: {series_id}")
                return None

            # Validate API data has essential fields
            required_fields = ['title_en', 'description']
            missing_fields = [field for field in required_fields if not mapped_data.get(field)]

            if missing_fields:
                LOGGER.warning(f"API data missing required fields: {missing_fields} for ID: {series_id}")
                return None  # Return None to trigger HTML fallback

            # Create show item
            show = self._create_show_item(url, lastmod)

            # Update with API data
            for key, value in mapped_data.items():
                if value is not None:
                    show[key] = value

            # Process episode URLs if available in API data
            if 'seasons' in mapped_data and mapped_data['seasons']:
                # Ensure episode_urls is populated from seasons data
                episode_urls = set()
                for season in show.get('seasons', []):
                    for episode in season.get('episodes', []):
                        if 'url' in episode and episode['url']:
                            episode_urls.add(self.normalize_url(episode['url']))

                show['episode_urls'] = list(episode_urls)

                # Set episode_count if not already set
                if not show.get('episode_count'):
                    show['episode_count'] = len(episode_urls)

            # Ensure we have required fields
            if not show.get('title_en'):
                # Extract from URL as last resort
                slug = show['url'].rstrip('/').split('/')[-1]
                show['title_en'] = slug.replace('-', ' ').title()
                LOGGER.warning(f"Using slug as title: {show['title_en']}")

            # If no seasons data yet, try to extract from HTML as fallback
            if not show.get('seasons') or not show.get('episode_urls'):
                LOGGER.info(f"API doesn't have complete seasons/episodes data, fetching HTML for {url}")
                html = fetch_sync(url, content_type=CONTENT_TYPE, lastmod=lastmod, modified_after=self.modified_after)
                if html:
                    selector = scrapy.Selector(text=html)
                    self._extract_seasons_and_episodes(show, selector)

            # Log the result
            self._log_extraction_result(show)

            return show

        except Exception as e:
            LOGGER.error(f"Error processing API series {series_id}: {e}", exc_info=True)
            # Record error for potential blacklisting
            if self.error_tracker:
                api_url = f"{API_BASE_URL}/tvshows/{series_id}"
                self.error_tracker.record_error(api_url, "api_error")
            return None

    def _process_html_series(self, response, lastmod=None) -> Optional[ShowItem]:
        """
        Process a series using HTML parsing.

        Args:
            response: Scrapy response object
            lastmod: Last modification timestamp from sitemap

        Returns:
            ShowItem or None if parsing fails
        """
        url = self.normalize_url(response.url)

        try:
            # Create show item and extract data
            show = self._create_show_item(url, lastmod)

            # Extract metadata using Scrapy selectors
            self._extract_title(show, response)
            self._extract_poster(show, response)
            self._extract_metadata(show, response)
            self._extract_genres(show, response)
            self._extract_people(show, response)

            # Extract seasons and episode URLs
            self._extract_seasons_and_episodes(show, response)

            # If using API-first, try to extract API ID
            if self.api_first and self.api_client:
                api_id = self.api_client.extract_id_from_url(url)
                if api_id:
                    show['api_id'] = api_id
                    show['api_source'] = 'html_extraction'

            # Log the result
            self._log_extraction_result(show)

            return show

        except Exception as e:
            LOGGER.error(f"Error parsing HTML series {url}: {e}", exc_info=True)
            # Record error for potential blacklisting
            if self.error_tracker:
                self.error_tracker.record_error(url, "html_parsing_error")
            return None

    def _create_show_item(self, url: str, lastmod: Optional[str]) -> ShowItem:
        """
        Create a new ShowItem with initial values.

        Args:
            url: The series URL
            lastmod: Last modification timestamp from sitemap

        Returns:
            Initialized ShowItem
        """
        return ShowItem(
            url=self.normalize_url(url),
            sitemap_url=self.normalize_url(url),
            lastmod=lastmod,
            is_new=True,
            is_modified=False,  # Will be set to True for items modified after filter date
            genres=[],
            directors=[],
            cast=[],
            seasons=[],
            episode_urls=[],
            language_code='fa'  # Default to Farsi for Farsiland
        )

    def _extract_title(self, show: ShowItem, response) -> None:
        """
        Extract the series title from the HTML.

        Args:
            show: Show item to update
            response: Scrapy response object or Selector
        """
        try:
            # Prioritize selectors based on user feedback
            title_selectors = [
                ".data h1::text", # User specified
                "h1::text",
                ".entry-title::text",
                "meta[property='og:title']::attr(content)",
                ".sheader .shead h1::text",
                ".data h1::text" # Already present, keeping for robustness
            ]

            for selector in title_selectors:
                title_text = response.css(selector).get()
                if title_text:
                    show['title_en'] = self.normalize_text(title_text.strip())
                    break

            # Prioritize Farsi title selectors based on user feedback
            farsi_title_selectors = [
                ".data h2::text", # User specified
                ".originalTitle::text",
                "span[itemprop='alternativeHeadline']::text"
            ]

            for selector in farsi_title_selectors:
                farsi_title = response.css(selector).get()
                if farsi_title:
                    show['title_fa'] = self.normalize_text(farsi_title.strip())
                    break

            # Fallback if no title found
            if not show.get('title_en'):
                # Extract from URL as last resort
                slug = show['url'].rstrip('/').split('/')[-1]
                show['title_en'] = slug.replace('-', ' ').title()
                LOGGER.warning(f"Could not extract title for {show['url']}, using slug: {show['title_en']}")

        except Exception as e:
            LOGGER.warning(f"Error extracting title: {e}")
            # Set a default title based on URL
            slug = show['url'].rstrip('/').split('/')[-1]
            show['title_en'] = slug.replace('-', ' ').title()

    def _extract_poster(self, show: ShowItem, response) -> None:
        """
        Extract the series poster image URL.

        Args:
            show: Show item to update
            response: Scrapy response object or Selector
        """
        try:
            # Prioritize selectors based on user feedback
            poster_selectors = [
                ".poster img::attr(src)", # User specified
                ".thumb img::attr(src)",
                "meta[property='og:image']::attr(content)",
                ".imagen img::attr(src)"
            ]

            # Try data-src attributes as well
            data_src_selectors = [
                ".poster img::attr(data-src)", # User specified potential data-src
                ".thumb img::attr(data-src)",
                ".imagen img::attr(data-src)"
            ]

            # First try regular src attributes
            for selector in poster_selectors:
                poster_url = response.css(selector).get()
                if poster_url:
                    show['poster'] = poster_url
                    break

            # If not found, try data-src attributes
            if not show.get('poster'):
                for selector in data_src_selectors:
                    poster_url = response.css(selector).get()
                    if poster_url:
                        show['poster'] = poster_url
                        break

            # Ensure poster URL is absolute
            if show.get('poster') and not show['poster'].startswith(('http://', 'https://')):
                show['poster'] = urljoin(show['url'], show['poster'])

        except Exception as e:
            LOGGER.warning(f"Error extracting poster: {e}")
            show['poster'] = None

    def _extract_metadata(self, show: ShowItem, response) -> None:
        """
        Extract general metadata like description, ratings, dates.

        Args:
            show: Show item to update
            response: Scrapy response object or Selector
        """
        try:
            # Extract description - Prioritize user specified selector
            description_selectors = [
                ".wp-content p::text", # User specified
                "meta[name='description']::attr(content)",
                "meta[property='og:description']::attr(content)",
                ".description::text",
                "div[itemprop='description']::text"
            ]

            for selector in description_selectors:
                desc_texts = response.css(selector).getall()
                if desc_texts:
                    show['description'] = " ".join([self.normalize_text(t.strip()) for t in desc_texts])
                    break

            # If no single selector matches, try combining paragraphs (this was already a fallback, keeping it)
            if not show.get('description'):
                desc_paragraphs = response.css(".wp-content p::text").getall()
                if desc_paragraphs:
                    show['description'] = " ".join([self.normalize_text(p.strip()) for p in desc_paragraphs])


            # Extract first air date (keeping existing selectors as user didn't specify new ones)
            date_selectors = [
                "span.date::text",
                ".extra span.date::text",
                "meta[property='og:release_date']::attr(content)",
                ".extra .date::text"
            ]

            for selector in date_selectors:
                date_text = response.css(selector).get()
                if date_text:
                    show['first_air_date'] = self.normalize_text(date_text.strip())
                    break

            # Extract rating (keeping existing selectors as user didn't specify new ones)
            try:
                rating_selectors = [
                    ".imdb span::text",
                    "span[itemprop='ratingValue']::text",
                    ".rating_number::text"
                ]

                for selector in rating_selectors:
                    rating_text = response.css(selector).get()
                    if rating_text:
                        # Extract numeric part with support for Persian digits
                        rating_text = self.normalize_text(rating_text)
                        rating_match = re.search(r'(\d+(\.\d+)?)', rating_text)
                        if rating_match:
                            show['rating'] = float(rating_match.group(1))
                            break

                # Extract rating count (keeping existing selectors)
                vote_selectors = [
                    ".imdb span.votes::text",
                    "span[itemprop='ratingCount']::text"
                ]

                for selector in vote_selectors:
                    vote_text = response.css(selector).get()
                    if vote_text:
                        # Extract numeric part, may contain comma separators
                        vote_text = self.normalize_text(vote_text)
                        votes_match = re.search(r'([\d,]+)', vote_text)
                        if votes_match:
                            show['rating_count'] = int(votes_match.group(1).replace(',', ''))
                            break
            except Exception as e:
                LOGGER.debug(f"Error extracting ratings: {e}")
                # Non-critical, can continue

        except Exception as e:
            LOGGER.warning(f"Error extracting metadata: {e}")

    def _extract_genres(self, show: ShowItem, response) -> None:
        """
        Extract genre information.

        Args:
            show: Show item to update
            response: Scrapy response object or Selector
        """
        try:
            genres = []

            # Prioritize selectors based on user feedback
            genre_selectors = [
                ".sgeneros a::text", # User specified
                "span[itemprop='genre']::text",
                ".genres a::text",
                ".metadataContent span.genres a::text"
            ]

            for selector in genre_selectors:
                genre_texts = response.css(selector).getall()
                if genre_texts:
                    for text in genre_texts:
                        genre_text = self.normalize_text(text.strip())
                        if genre_text and genre_text not in genres:
                            genres.append(genre_text)
                    break

            # Fallback if no genres found from prioritized selectors
            if not genres:
                 fallback_genre_selectors = [
                    ".sgeneros a::text",
                    "span[itemprop='genre']::text",
                    ".genres a::text",
                    ".metadataContent span.genres a::text"
                ]
                 for selector in fallback_genre_selectors:
                    genre_texts = response.css(selector).getall()
                    if genre_texts:
                        for text in genre_texts:
                            genre_text = self.normalize_text(text.strip())
                            if genre_text and genre_text not in genres:
                                genres.append(genre_text)
                        if genres: # If found using fallback, stop
                            break


            show['genres'] = genres

        except Exception as e:
            LOGGER.warning(f"Error extracting genres: {e}")
            show['genres'] = []

    def _extract_people(self, show: ShowItem, response) -> None:
        """
        Extract director and cast information.

        Args:
            show: Show item to update
            response: Scrapy response object or Selector
        """
        try:
            # Extract directors - Prioritize selectors within #cast if applicable
            directors = []
            director_selectors = [
                "#cast .person[itemprop='director'] .name::text", # Targeting within #cast
                "#cast .director a::text", # Targeting within #cast
                "#cast span[itemprop='director']::text", # Targeting within #cast
                ".person[itemprop='director'] .name::text", # Original fallback
                ".director a::text", # Original fallback
                "span[itemprop='director']::text" # Original fallback
            ]

            for selector in director_selectors:
                director_texts = response.css(selector).getall()
                if director_texts:
                    for text in director_texts:
                        name = self.normalize_text(text.strip())
                        if name and name not in directors:
                            directors.append(name)
                    if directors: # If found using this selector, stop
                        break

            show['directors'] = directors

            # Extract cast members - Prioritize selectors within #cast if applicable
            cast = []
            cast_selectors = [
                "#cast .person[itemprop='actor'] .name::text", # Targeting within #cast
                "#cast .cast a::text", # Targeting within #cast
                "#cast span[itemprop='actor']::text", # Targeting within #cast
                ".person[itemprop='actor'] .name::text", # Original fallback
                ".cast a::text", # Original fallback
                "span[itemprop='actor']::text" # Original fallback
            ]

            for selector in cast_selectors:
                cast_texts = response.css(selector).getall()
                if cast_texts:
                    for text in cast_texts:
                        name = self.normalize_text(text.strip())
                        if name and name not in cast:
                            cast.append(name)
                    if cast: # If found using this selector, stop
                        break

            show['cast'] = cast

        except Exception as e:
            LOGGER.warning(f"Error extracting people: {e}")
            show['directors'] = []
            show['cast'] = []

    def _extract_seasons_and_episodes(self, show: ShowItem, response) -> None:
        """
        Extract seasons and episode URLs without processing episode pages.

        Args:
            show: Show item to update
            response: Scrapy response object or Selector
        """
        try:
            seasons = []
            episode_urls = set()  # To track all unique episode URLs

            # Find season containers
            season_containers = response.css("div.se-c")
            if not season_containers:
                LOGGER.debug(f"No season containers found for {show['url']}")

                # Look for alternative season layout
                alt_containers = response.css(".seasons .se-c")
                if alt_containers:
                    season_containers = alt_containers
                else:
                    # Try another fallback
                    alt_containers = response.css(".temporadas > div")
                    if alt_containers:
                        season_containers = alt_containers

            # Memory optimization in low memory mode - process only one container at a time
            if self.low_memory and len(season_containers) > 5:
                LOGGER.info(f"Low memory mode: processing {len(season_containers)} seasons carefully")

            # Track total episode count
            total_episode_count = 0

            # Process each season
            for i, season_div in enumerate(season_containers, 1):
                # Extract season number
                season_header_text = season_div.css(".se-q .se-t::text").get()

                try:
                    if season_header_text:
                        # Try to parse season number from text
                        season_header_text = self.normalize_text(season_header_text)
                        season_match = re.search(r'(\d+)', season_header_text)
                        if season_match:
                            season_number = int(season_match.group(1))
                        else:
                            season_number = i
                    else:
                        season_number = i
                except Exception as e:
                    LOGGER.debug(f"Error parsing season number: {e}")
                    season_number = i

                # Find episode list for this season
                episode_list = season_div.css("ul.episodios > li")
                if not episode_list:
                    # Try alternative structure
                    episode_list = season_div.css(".se-a ul > li")

                # Memory optimization - if too many episodes, batch process them
                if self.low_memory and len(episode_list) > 50:
                    LOGGER.info(f"Low memory mode: processing {len(episode_list)} episodes in batches")
                    # Process in batches of 50
                    batch_size = 50
                    episode_list = list(episode_list)  # Convert to list for slicing
                    batch_results = []

                    for batch_start in range(0, len(episode_list), batch_size):
                        batch_end = min(batch_start + batch_size, len(episode_list))
                        batch = episode_list[batch_start:batch_end]

                        batch_data = self._process_episode_batch(batch, season_number, show['url'])
                        batch_results.extend(batch_data['episodes'])
                        episode_urls.update(batch_data['urls'])
                        total_episode_count += len(batch_data['episodes'])

                        # Allow GC after each batch in low memory mode
                        if self.low_memory:
                            gc.collect()

                    # Store season data
                    if batch_results:
                        season_data = {
                            "season_number": season_number,
                            "title": f"Season {season_number}",
                            "episode_count": len(batch_results),
                            "episodes": batch_results
                        }
                        seasons.append(season_data)
                else:
                    # Normal processing for reasonable amount of episodes
                    # Store episode data without processing episode pages
                    season_episodes = []

                    for ep_li in episode_list:
                        try:
                            # Find the episode link
                            ep_url = ep_li.css(".episodiotitle a::attr(href)").get()
                            if not ep_url:
                                # Try alternative selectors
                                ep_url = ep_li.css("a::attr(href)").get()
                                if not ep_url:
                                    continue

                            # Get the episode URL and normalize
                            ep_url = self.normalize_url(urljoin(BASE_URL, ep_url))

                            # Extract episode number
                            numerando_text = ep_li.css(".numerando::text").get()
                            if numerando_text and "-" in numerando_text:
                                try:
                                    numerando_text = self.normalize_text(numerando_text)
                                    ep_number = int(numerando_text.split("-")[1].strip())
                                except (ValueError, IndexError):
                                    # Try to extract from URL
                                    ep_match = re.search(r'ep(\d+)', ep_url.lower())
                                    if ep_match:
                                        ep_number = int(ep_match.group(1))
                                    else:
                                        ep_number = len(season_episodes) + 1
                            else:
                                # Try to extract from URL
                                ep_match = re.search(r'ep(\d+)', ep_url.lower())
                                if ep_match:
                                    ep_number = int(ep_match.group(1))
                                else:
                                    ep_number = len(season_episodes) + 1

                            # Extract episode title
                            ep_title = ep_li.css(".episodiotitle a::text").get()
                            if ep_title:
                                ep_title = self.normalize_text(ep_title.strip())
                            else:
                                ep_title = f"Episode {ep_number}"

                            # Extract air date if available
                            ep_date = ep_li.css(".episodiotitle .date::text").get()
                            if ep_date:
                                ep_date = self.normalize_text(ep_date.strip())

                            # Get thumbnail if available
                            thumb_url = None
                            thumb_selectors = [
                                ".thumb img::attr(src)",
                                ".thumb img::attr(data-src)",
                                ".thumb img::attr(data-lazy-src)"
                            ]

                            for selector in thumb_selectors:
                                thumb_url = ep_li.css(selector).get()
                                if thumb_url:
                                    if not thumb_url.startswith(('http://', 'https://')):
                                        thumb_url = urljoin(BASE_URL, thumb_url)
                                    break

                            # Store episode data
                            episode_data = {
                                "episode_number": ep_number,
                                "title": ep_title,
                                "date": ep_date,
                                "url": ep_url,
                                "thumbnail": thumb_url
                            }

                            season_episodes.append(episode_data)
                            episode_urls.add(ep_url)
                            total_episode_count += 1

                        except Exception as e:
                            LOGGER.warning(f"Error processing episode in season {season_number}: {e}")

                    # Skip empty seasons
                    if not season_episodes:
                        continue

                    # Sort episodes by number
                    season_episodes.sort(key=lambda ep: ep.get('episode_number', 0))

                    # Add season data
                    season_data = {
                        "season_number": season_number,
                        "title": f"Season {season_number}",
                        "episode_count": len(season_episodes),
                        "episodes": season_episodes
                    }

                    seasons.append(season_data)

                # Allow GC after each season in low memory mode
                if self.low_memory and i % 3 == 0:
                    gc.collect()

            # Sort seasons by number
            seasons.sort(key=lambda s: s.get('season_number', 0))

            # If no seasons found from structured layout, create a default season
            if not seasons and episode_urls:
                # Create a default season with all found episodes
                default_season = {
                    "season_number": 1,
                    "title": "Season 1",
                    "episode_count": len(episode_urls),
                    "episodes": [{"url": url} for url in episode_urls]
                }
                seasons.append(default_season)

            # Update show with season and episode data
            show['seasons'] = seasons
            show['season_count'] = len(seasons)
            show['episode_count'] = total_episode_count

            # Store all unique episode URLs
            show['episode_urls'] = list(episode_urls)

            LOGGER.info(f"Extracted {len(seasons)} seasons with {total_episode_count} episodes")

        except Exception as e:
            LOGGER.error(f"Error extracting seasons and episodes: {e}", exc_info=True)
            # Ensure we at least have empty lists to avoid further errors
            show['seasons'] = []
            show['season_count'] = 0
            show['episode_count'] = 0
            show['episode_urls'] = []

    def _process_episode_batch(self, episode_list, season_number, show_url):
        """
        Process a batch of episodes to optimize memory usage.

        Args:
            episode_list: List of episode elements to process
            season_number: Season number for these episodes
            show_url: URL of the parent show

        Returns:
            Dict with 'episodes' list and 'urls' set
        """
        result = {
            'episodes': [],
            'urls': set()
        }

        for ep_li in episode_list:
            try:
                # Find the episode link
                ep_url = ep_li.css(".episodiotitle a::attr(href)").get()
                if not ep_url:
                    # Try alternative selectors
                    ep_url = ep_li.css("a::attr(href)").get()
                    if not ep_url:
                        continue

                # Get the episode URL and normalize
                ep_url = self.normalize_url(urljoin(show_url, ep_url))

                # Extract episode number
                numerando_text = ep_li.css(".numerando::text").get()
                ep_number = len(result['episodes']) + 1  # Default

                if numerando_text and "-" in numerando_text:
                    try:
                        numerando_text = self.normalize_text(numerando_text)
                        ep_number = int(numerando_text.split("-")[1].strip())
                    except (ValueError, IndexError):
                        # Try to extract from URL
                        ep_match = re.search(r'ep(\d+)', ep_url.lower())
                        if ep_match:
                            ep_number = int(ep_match.group(1))

                # Extract episode title
                ep_title = ep_li.css(".episodiotitle a::text").get()
                if ep_title:
                    ep_title = self.normalize_text(ep_title.strip())
                else:
                    ep_title = f"Episode {ep_number}"

                # Extract air date if available
                ep_date = ep_li.css(".episodiotitle .date::text").get()
                if ep_date:
                    ep_date = self.normalize_text(ep_date.strip())

                # Get thumbnail if available
                thumb_url = None
                thumb_selectors = [
                    ".thumb img::attr(src)",
                    ".thumb img::attr(data-src)",
                    ".thumb img::attr(data-lazy-src)"
                ]

                for selector in thumb_selectors:
                    thumb_url = ep_li.css(selector).get()
                    if thumb_url:
                        if not thumb_url.startswith(('http://', 'https://')):
                            thumb_url = urljoin(show_url, thumb_url)
                        break

                # Store episode data
                episode_data = {
                    "episode_number": ep_number,
                    "title": ep_title,
                    "date": ep_date,
                    "url": ep_url,
                    "thumbnail": thumb_url
                }

                result['episodes'].append(episode_data)
                result['urls'].add(ep_url)

            except Exception as e:
                LOGGER.warning(f"Error processing episode batch item in season {season_number}: {e}")

        return result

    def _log_extraction_result(self, show: ShowItem) -> None:
        """
        Log the result of the extraction process.

        Args:
            show: The extracted show item
        """
        title = show.get('title_en', 'Unknown')
        seasons = show.get('season_count', 0)
        episodes = show.get('episode_count', 0)

        source = "API" if show.get('api_id') else "HTML"
        is_modified = "modified" if show.get('is_modified') else "new/unchanged"
        LOGGER.info(f"Extracted ({source}): {title} ({seasons} seasons, {episodes} episodes) - {is_modified}")

    def _is_series_url(self, url: str) -> bool:
        """
        Check if a URL is a valid series page.

        Args:
            url: URL to check

        Returns:
            True if the URL is a valid series page
        """
        if not url:
            return False

        # Check if URL is blacklisted
        if self.error_tracker and self.error_tracker.should_skip(url):
            return False

        # Use regular expression to validate URL format
        return bool(re.match(SERIES_URL_PATTERN, url))

    def closed(self, reason):
        """
        Clean up resources when spider closes.

        Args:
            reason: Close reason
        """
        LOGGER.info(f"Series spider closing: {reason}")

        # Save final checkpoint
        if self.checkpoint_manager:
            self.checkpoint_manager.save_checkpoint(force=True)
            LOGGER.info("Final checkpoint saved")

        # Clear API client cache
        if self.api_client:
            try:
                # Clear cache to free memory
                if hasattr(self.api_client, 'clear_cache'):
                    self.api_client.clear_cache()
                    LOGGER.info("API client cache cleared")
            except Exception as e:
                LOGGER.warning(f"Error cleaning up API client: {e}")

        # Print memory stats in debug mode
        if LOGGER.level <= logging.DEBUG and hasattr(self, 'processed_count'):
            LOGGER.debug(f"Series spider processed {self.processed_count} items")

            # Try to get memory usage
            try:
                import psutil
                process = psutil.Process()
                memory_info = process.memory_info()
                LOGGER.debug(f"Memory usage: {memory_info.rss / (1024*1024):.1f} MB")
            except ImportError:
                pass

        # Force garbage collection
        gc.collect()
        LOGGER.info("Resources cleaned up")
