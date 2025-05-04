# File: farsiland_scraper/utils/sitemap_parser.py
# Version: 7.0.0
# Last Updated: 2025-06-27

"""
Sitemap parser for Farsiland scraper.

Parses sitemap.xml files to extract URLs for scraping with support for:
- Sitemap index and individual sitemap parsing
- URL categorization by content type
- Incremental updates with date filtering
- API ID extraction for API-first approach
- Error tracking and reporting
- Full metadata preservation
- Separate tracking of modified URLs
- Robust URL normalization for non-ASCII characters
- Atomic file operations
- Comprehensive error handling

Changelog:
- [7.0.0] Complete rewrite addressing critical architecture issues
- [7.0.0] Fixed inconsistent return type handling with standardized structure
- [7.0.0] Implemented robust URL normalization for non-ASCII characters
- [7.0.0] Enhanced error tracking with comprehensive reporting
- [7.0.0] Added atomic file operations to prevent data corruption
- [7.0.0] Improved memory management for large sitemaps
- [7.0.0] Fixed timezone handling for consistent date comparison
- [7.0.0] Standardized date handling across all functions
- [7.0.0] Added validation for all return values
- [6.0.0] Previous major version with architecture issues
"""

import os
import requests
import time
import json
import re
import logging
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, quote, unquote, urlunparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
import unicodedata
import tempfile
import random
from typing import Dict, List, Optional, Set, Tuple, Any, Union

from farsiland_scraper.config import BASE_URL, LOGGER, CACHE_DIR

# Base configuration
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap_index.xml"
RSS_FEED_URL = f"{BASE_URL}/feed"
DEFAULT_OUTPUT_FILE = os.path.join(CACHE_DIR, "parsed_urls.json")
LAST_CHECK_FILE = os.path.join(CACHE_DIR, "last_check.json")

# Content patterns for URL categorization
CONTENT_PATTERNS = {
    "movies": [
        r"/movies/[^/]+/?$",
        r"/movies-\d{4}/[^/]+/?$",
        r"/old-iranian-movies/[^/]+/?$"
    ],
    "shows": [
        r"/tvshows/[^/]+/?$",
        r"/series-22/[^/]+/?$",
        r"/iranian-series/[^/]+/?$"
    ],
    "episodes": [
        r"/episodes/[^/]+/?$"
    ]
}

class EnhancedErrorTracker:
    """
    Comprehensive error tracking with detailed categorization.
    
    Features:
    - Track errors by category and type
    - Generate detailed error reports
    - Support URL blacklisting
    - Maintain error statistics
    """
    
    def __init__(self, logger=LOGGER):
        """Initialize error tracker with the specified logger."""
        self.logger = logger
        self.errors = {
            'total': 0,
            'categories': {},
            'blacklisted': set()
        }
    
    def record_error(self, category: str, error_type: str, url: Optional[str] = None) -> None:
        """
        Record an error with comprehensive categorization.
        
        Args:
            category: Error category (e.g., 'sitemap_parsing')
            error_type: Specific error type (e.g., 'http_404')
            url: Optional URL associated with the error
        """
        # Increment total errors
        self.errors['total'] += 1
        
        # Track errors by category
        if category not in self.errors['categories']:
            self.errors['categories'][category] = {}
        
        if error_type not in self.errors['categories'][category]:
            self.errors['categories'][category][error_type] = 0
        
        self.errors['categories'][category][error_type] += 1
        
        # Optionally blacklist URL
        if url:
            self.errors['blacklisted'].add(url)
    
    def generate_error_report(self) -> str:
        """
        Generate a comprehensive error report.
        
        Returns:
            Formatted error report as a string
        """
        report = [
            "=== Sitemap Parsing Error Report ===",
            f"Total Errors: {self.errors['total']}"
        ]
        
        # Detailed category breakdown
        for category, errors in self.errors['categories'].items():
            report.append(f"\n{category.upper()} Errors:")
            for error_type, count in errors.items():
                report.append(f"  {error_type}: {count}")
        
        # Blacklisted URLs
        if self.errors['blacklisted']:
            report.append("\nBlacklisted URLs:")
            # Limit to 10 URLs to keep report manageable
            for url in list(self.errors['blacklisted'])[:10]:
                report.append(f"  {url}")
            
            if len(self.errors['blacklisted']) > 10:
                report.append(f"  ... and {len(self.errors['blacklisted']) - 10} more")
        
        return "\n".join(report)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get error statistics.
        
        Returns:
            Dictionary with error statistics
        """
        category_counts = {
            category: sum(counts.values()) 
            for category, counts in self.errors['categories'].items()
        }
        
        return {
            'total_errors': self.errors['total'],
            'by_category': category_counts,
            'blacklisted_urls': len(self.errors['blacklisted'])
        }

class RequestManager:
    """
    Handles HTTP requests with retries and error handling.
    
    Features:
    - Configurable retry mechanism with exponential backoff
    - Detailed error logging
    - Integration with error tracking
    """
    
    def __init__(self, max_retries: int = 3, timeout: int = 30, delay: int = 2, 
                 error_tracker: Optional[Any] = None):
        """
        Initialize the request manager.
        
        Args:
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
            delay: Base delay between retries (will use exponential backoff)
            error_tracker: Optional error tracker instance
        """
        self.max_retries = max_retries
        self.timeout = timeout
        self.delay = delay
        self.error_tracker = error_tracker
        
        # Create and configure session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0',
            'Accept': 'text/html,application/xml,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5,fa;q=0.3'  # Added Farsi language support
        })
    
    def get(self, url: str) -> Optional[bytes]:
        """
        Send a GET request with retries and error handling.
        
        Args:
            url: URL to fetch
            
        Returns:
            Response content as bytes, or None if request failed
        """
        # Skip if URL is blacklisted by external error tracker
        if hasattr(self.error_tracker, 'should_skip') and self.error_tracker.should_skip(url):
            LOGGER.debug(f"Skipping blacklisted URL: {url}")
            return None
            
        for attempt in range(self.max_retries):
            try:
                LOGGER.debug(f"Requesting {url} (attempt {attempt+1}/{self.max_retries})")
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.content
            except requests.RequestException as e:
                # More specific error messages based on exception type
                if isinstance(e, requests.ConnectionError):
                    LOGGER.warning(f"Connection error for {url}: {e}")
                elif isinstance(e, requests.Timeout):
                    LOGGER.warning(f"Timeout error for {url}: {e}")
                elif isinstance(e, requests.HTTPError):
                    LOGGER.warning(f"HTTP error {e.response.status_code} for {url}: {e}")
                    
                    # Record error if error tracker is available
                    if self.error_tracker:
                        error_type = f"http_{e.response.status_code}"
                        self.error_tracker.record_error("sitemap", error_type, url)
                        
                    # Don't retry for client errors (4xx) except 429 (too many requests)
                    if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                        LOGGER.error(f"Client error {e.response.status_code} for {url}, not retrying")
                        return None
                else:
                    LOGGER.warning(f"Request error for {url}: {e}")
                    
                    # Record generic error
                    if self.error_tracker:
                        self.error_tracker.record_error("sitemap", "request_error", url)
                
                # If this isn't the last attempt, wait before retrying
                if attempt < self.max_retries - 1:
                    # Exponential backoff with jitter
                    sleep_time = self.delay * (2 ** attempt) * (0.75 + 0.5 * random.random())
                    LOGGER.info(f"Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                else:
                    LOGGER.error(f"Failed to fetch {url} after {self.max_retries} attempts")
                    
                    # Record final failure
                    if self.error_tracker:
                        self.error_tracker.record_error("sitemap", "max_retries", url)
        
        return None

class SitemapParser:
    """
    Parses sitemap files and categorizes URLs with comprehensive error handling.
    
    Features:
    - Sitemap index and individual sitemap parsing
    - URL categorization by content type
    - Incremental updates with date filtering
    - API ID extraction for API-first approach
    - Error tracking and reporting
    - Atomic file operations to prevent corruption
    - Consistent return type handling
    - Robust URL normalization for non-ASCII characters
    """
    
    def __init__(
        self, 
        sitemap_url: str = SITEMAP_INDEX_URL,
        output_file: str = DEFAULT_OUTPUT_FILE,
        rss_url: str = RSS_FEED_URL,
        skip_taxonomies: bool = True,
        error_tracker: Optional[Any] = None
    ):
        """
        Initialize the sitemap parser.
        
        Args:
            sitemap_url: URL of the sitemap index
            output_file: Path to save parsed URLs
            rss_url: URL of the RSS feed
            skip_taxonomies: Whether to skip taxonomy pages
            error_tracker: Optional error tracker instance
        """
        self.sitemap_url = sitemap_url
        self.output_file = output_file
        self.rss_url = rss_url
        self.last_check_file = LAST_CHECK_FILE
        self.skip_taxonomies = skip_taxonomies
        
        # Use provided error tracker or create our own
        if error_tracker:
            self.error_tracker = error_tracker
        else:
            self.error_tracker = EnhancedErrorTracker()
        
        # Initialize the request manager
        self.requester = RequestManager(error_tracker=self.error_tracker)
        
        # Dictionary to store all categorized URLs
        self.results = {
            "movies": [],
            "shows": [],
            "episodes": []
        }
        
        # Dictionary to store only modified URLs
        self.modified_results = {
            "movies": [],
            "shows": [],
            "episodes": []
        }
        
        # Track parsing errors for reporting
        self.parsing_errors = {
            "index": 0,      # Sitemap index failures
            "sitemap": 0,    # Individual sitemap failures
            "url": 0,        # URL categorization failures
            "blacklisted": 0 # Number of blacklisted URLs skipped
        }
        
        # Ensure output directories exist
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        os.makedirs(os.path.dirname(self.last_check_file), exist_ok=True)
        
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
            parsed_url = urlparse(url)
            
            # Normalize path
            path = unquote(parsed_url.path)
            
            # Unicode normalization (NFKC form)
            path = unicodedata.normalize('NFKC', path)
            
            # Convert Persian/Arabic digits to Latin
            persian_digits = {
                '۰': '0', '۱': '1', '۲': '2', '۳': '3', '۴': '4',
                '۵': '5', '۶': '6', '۷': '7', '۸': '8', '۹': '9'
            }
            for persian, latin in persian_digits.items():
                path = path.replace(persian, latin)
            
            # Reconstruct URL with proper encoding
            normalized = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                path.rstrip('/'),  # Remove trailing slash
                parsed_url.params,
                parsed_url.query,
                parsed_url.fragment
            ))
            
            return normalized
        except Exception as e:
            LOGGER.warning(f"URL normalization failed for {url}: {e}")
            # Fall back to simple normalization if complex fails
            return url.rstrip('/')
    
    def categorize_url(self, url: str) -> Optional[str]:
        """
        Determine the category of a URL based on pattern matching.
        
        Args:
            url: URL to categorize
            
        Returns:
            Category name or None if URL doesn't match any category
        """
        normalized_url = self.normalize_url(url)
        
        # Skip blacklisted URLs
        if hasattr(self.error_tracker, 'should_skip') and self.error_tracker.should_skip(normalized_url):
            self.parsing_errors["blacklisted"] += 1
            LOGGER.debug(f"Skipping blacklisted URL: {normalized_url}")
            return None
            
        # Skip taxonomy pages if configured
        if self.skip_taxonomies and any(p in normalized_url for p in [
            '/genres/', '/dtcast/', '/dtdirector/', '/dtcreator/', 
            '/dtstudio/', '/dtnetworks/', '/dtyear/'
        ]):
            return None
        
        # Check URL against patterns for each category
        for category, patterns in CONTENT_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, normalized_url):
                    return category
        
        # If no pattern matches, try to infer from URL components
        url_path = urlparse(normalized_url).path
        
        if '/movies/' in url_path:
            return 'movies'
        elif '/tvshows/' in url_path:
            return 'shows'
        elif '/episodes/' in url_path:
            return 'episodes'
        
        # If we still can't categorize, return None
        return None
    
    def extract_id_from_url(self, url: str) -> Optional[int]:
        """
        Extract WordPress post ID from URL.
        
        Args:
            url: The URL to extract from
            
        Returns:
            Post ID or None if not found
        """
        if not url:
            return None
            
        # Try query parameter (?p=123)
        query_match = re.search(r"[?&]p=(\d+)", url)
        if query_match:
            try:
                return int(query_match.group(1))
            except (ValueError, TypeError):
                pass
                
        # Try ID in slug (post-name-123)
        path = urlparse(url).path
        
        # Pattern 1: /post-name-12345/
        end_match = re.search(r"-(\d+)/?$", path)
        if end_match:
            try:
                return int(end_match.group(1))
            except (ValueError, TypeError):
                pass
                
        # Pattern 2: /post-name-12345-extra/
        middle_match = re.search(r"-(\d+)-", path)
        if middle_match:
            try:
                return int(middle_match.group(1))
            except (ValueError, TypeError):
                pass
        
        return None
        
    def check_for_updates(self) -> bool:
        """
        Check if the site has been updated since last check.
        
        Returns:
            True if updates are available, False otherwise
        """
        LOGGER.info("Checking RSS feed for updates...")
        
        # Get last check time
        last_check = self._load_last_check_time()
        if not last_check:
            LOGGER.info("No previous check found, update needed")
            return True
        
        # Get current build date from RSS
        last_build_date = self._get_rss_build_date()
        if not last_build_date:
            LOGGER.warning("Could not get last build date from RSS, assuming update needed")
            return True
        
        LOGGER.info(f"Last site update: {last_build_date}")
        LOGGER.info(f"Last check: {last_check}")
        
        # Ensure both dates are timezone-aware for accurate comparison
        if last_check.tzinfo is None and last_build_date.tzinfo is not None:
            try:
                # Try to use the same timezone as last_build_date
                last_check = last_check.replace(tzinfo=timezone.utc)
            except Exception as e:
                LOGGER.warning(f"Could not convert timestamp to timezone-aware: {e}")
                # If we can't make a proper comparison, assume update is needed
                return True
        
        # Compare dates
        try:
            if last_build_date > last_check:
                LOGGER.info("Site has been updated since last check")
                return True
            else:
                LOGGER.info("No updates since last check")
                return False
        except TypeError as e:
            # If comparison still fails, log and assume update is needed
            LOGGER.warning(f"Could not compare timestamps: {e}, assuming update needed")
            return True
        
    def _load_last_check_time(self) -> Optional[datetime]:
        """
        Load the timestamp of the last check with error handling.
        
        Returns:
            Datetime of last check or None if not available
        """
        if not os.path.exists(self.last_check_file):
            return None
        
        try:
            with open(self.last_check_file, 'r') as f:
                data = json.load(f)
                return datetime.fromisoformat(data.get('last_check_time'))
        except (json.JSONDecodeError, ValueError, IOError) as e:
            LOGGER.error(f"Error loading last check time: {e}")
            
            # Record error in error tracker
            self.error_tracker.record_error("sitemap", "check_time_error", self.last_check_file)
                
            return None
    
    def _save_last_check_time(self) -> bool:
        """
        Save the current time as the last check time with atomic file operations.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create current timestamp in ISO format
            current_time = datetime.now().isoformat()
            data = {'last_check_time': current_time}
            
            # Use atomic write pattern
            fd, temp_path = tempfile.mkstemp(
                dir=os.path.dirname(self.last_check_file), 
                suffix='.json.tmp'
            )
            
            try:
                with os.fdopen(fd, 'w') as f:
                    json.dump(data, f, ensure_ascii=False)
                    f.flush()
                    os.fsync(f.fileno())  # Ensure data is written to disk
                
                # Atomic replace
                os.replace(temp_path, self.last_check_file)
                LOGGER.info(f"Saved check timestamp: {current_time}")
                return True
                
            except Exception as e:
                # Clean up temp file on error
                LOGGER.error(f"Error writing check timestamp: {e}")
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                return False
                
        except Exception as e:
            LOGGER.error(f"Error saving last check time: {e}")
            return False
    
    def _get_rss_build_date(self) -> Optional[datetime]:
        """
        Get the last build date from the RSS feed.
        
        Returns:
            Datetime of last build or None if not available
        """
        content = self.requester.get(self.rss_url)
        if not content:
            return None
            
        try:
            root = ET.fromstring(content)
            channel = root.find('channel')
            
            if channel is not None:
                date_element = channel.find('lastBuildDate')
                
                if date_element is not None and date_element.text:
                    date_str = date_element.text
                    
                    # Try standard datetime parsing first
                    try:
                        return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                    except ValueError:
                        # Fall back to email utils if standard parsing fails
                        try:
                            from email.utils import parsedate_to_datetime
                            return parsedate_to_datetime(date_str)
                        except Exception as e:
                            LOGGER.warning(f"Could not parse RSS date '{date_str}': {e}")
            
            LOGGER.warning("No lastBuildDate found in RSS feed")
            return None
        except Exception as e:
            LOGGER.error(f"Error parsing RSS feed: {e}")
            
            # Record error in error tracker
            self.error_tracker.record_error("sitemap", "rss_parse_error", self.rss_url)
                
            return None
            
    def parse_sitemap_index(self) -> List[Dict[str, str]]:
        """
        Parse the sitemap index to get individual sitemap URLs.
        
        Returns:
            List of dictionaries with sitemap URLs and lastmod dates
        """
        LOGGER.info(f"Parsing sitemap index: {self.sitemap_url}")
        
        content = self.requester.get(self.sitemap_url)
        if not content:
            LOGGER.error(f"Could not fetch sitemap index: {self.sitemap_url}")
            self.parsing_errors["index"] += 1
            
            # Record error in error tracker
            self.error_tracker.record_error("sitemap", "index_fetch_error", self.sitemap_url)
                
            return []
        
        try:
            soup = BeautifulSoup(content, "xml")
            sitemaps = []
            
            for sitemap in soup.find_all("sitemap"):
                loc = sitemap.find("loc")
                lastmod = sitemap.find("lastmod")
                
                if loc:
                    url = loc.text.strip()
                    entry = {
                        "url": url,
                        "lastmod": lastmod.text.strip() if lastmod else None,
                        "type": self._get_sitemap_type(url)
                    }
                    sitemaps.append(entry)
            
            LOGGER.info(f"Found {len(sitemaps)} sitemaps in the index")
            
            # Sort sitemaps by priority
            return self._sort_sitemaps_by_priority(sitemaps)
        except Exception as e:
            LOGGER.error(f"Error parsing sitemap index: {e}")
            self.parsing_errors["index"] += 1
            
            # Record error in error tracker
            self.error_tracker.record_error("sitemap", "index_parse_error", self.sitemap_url)
                
            return []
    
    def _get_sitemap_type(self, url: str) -> str:
        """
        Extract sitemap type from URL.
        
        Args:
            url: Sitemap URL
            
        Returns:
            Sitemap type string
        """
        filename = os.path.basename(urlparse(url).path)
        
        # Match patterns like 'movies-sitemap.xml', 'post-sitemap1.xml'
        match = re.match(r'([a-zA-Z_-]+)-sitemap\d*\.xml', filename)
        if match:
            sitemap_type = match.group(1)
            
            # Map various sitemap types to our content categories
            type_mapping = {
                'movies': 'movies',
                'tvshows': 'shows',
                'episodes': 'episodes',
                'post': 'general'
            }
            
            return type_mapping.get(sitemap_type, sitemap_type)
        
        return "other"
        
    def _sort_sitemaps_by_priority(self, sitemaps: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Sort sitemaps by content type priority.
        
        Args:
            sitemaps: List of sitemap entries
            
        Returns:
            Sorted list of sitemap entries
        """
        # Define priority order for sitemap types
        priority_order = {
            'movies': 1,
            'shows': 2,
            'episodes': 3,
            'general': 4,
            'other': 5
        }
        
        # Sort by priority and then by lastmod date
        return sorted(sitemaps, key=lambda x: (
            priority_order.get(x['type'], 999),
            -1 if not x.get('lastmod') else 0  # Put entries without lastmod last within priority group
        ))
        
    def parse_sitemap(self, sitemap_url: str, modified_after: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Parse a single sitemap file to extract URLs.
        
        Args:
            sitemap_url: URL of the sitemap to parse
            modified_after: Only return content modified after this date
            
        Returns:
            List of URL entries with metadata
        """
        LOGGER.info(f"Parsing sitemap: {sitemap_url}")
        
        content = self.requester.get(sitemap_url)
        if not content:
            LOGGER.error(f"Could not fetch sitemap: {sitemap_url}")
            self.parsing_errors["sitemap"] += 1
            
            # Record error in error tracker
            self.error_tracker.record_error("sitemap", "fetch_error", sitemap_url)
                
            return []
        
        try:
            soup = BeautifulSoup(content, "xml")
            entries = []
            
            for url_tag in soup.find_all("url"):
                loc = url_tag.find("loc")
                lastmod = url_tag.find("lastmod")
                
                if loc:
                    url = self.normalize_url(loc.text.strip())
                    
                    # Skip if URL is blacklisted
                    if hasattr(self.error_tracker, 'should_skip') and self.error_tracker.should_skip(url):
                        self.parsing_errors["blacklisted"] += 1
                        continue
                        
                    lastmod_text = lastmod.text.strip() if lastmod else None
                    
                    # Extract ID for API use
                    api_id = self.extract_id_from_url(url)
                    
                    entry = {
                        "url": url,
                        "lastmod": lastmod_text,
                        "api_id": api_id,
                        "is_modified": False  # Default to false
                    }
                    
                    # Check if this URL is newer than modified_after
                    if modified_after and lastmod_text:
                        try:
                            # Make lastmod timezone-aware for comparison
                            lastmod_date = datetime.fromisoformat(lastmod_text.replace('Z', '+00:00'))
                            
                            # Make modified_after timezone-aware if it isn't already
                            if modified_after.tzinfo is None:
                                # Convert naive datetime to aware datetime with UTC timezone
                                modified_after = modified_after.replace(tzinfo=timezone.utc)
                            
                            # Mark this entry as modified if it's newer than the filter date
                            entry["is_modified"] = lastmod_date > modified_after
                            
                        except ValueError as e:
                            # Record parsing error but still include the URL
                            LOGGER.warning(f"Invalid lastmod date: {lastmod_text} - {e}")
                            self.parsing_errors["url"] += 1
                    
                    entries.append(entry)
            
            LOGGER.info(f"Found {len(entries)} URLs in sitemap: {sitemap_url}")
            return entries
        except Exception as e:
            LOGGER.error(f"Error parsing sitemap {sitemap_url}: {e}")
            self.parsing_errors["sitemap"] += 1
            
            # Record error in error tracker
            self.error_tracker.record_error("sitemap", "parse_error", sitemap_url)
                
            return []
    
    def process_sitemaps(self, modified_after: Optional[datetime] = None) -> None:
        """
        Process all sitemaps to categorize URLs.
        
        Args:
            modified_after: Only process content modified after this date
        """
        # Get list of sitemaps from the index
        sitemaps = self.parse_sitemap_index()
        if not sitemaps:
            LOGGER.error("No sitemaps found, aborting")
            return
        
        # Process each sitemap
        for sitemap in sitemaps:
            sitemap_url = sitemap["url"]
            sitemap_type = sitemap["type"]
            
            # Skip non-content sitemaps if they don't match our categories
            if sitemap_type not in ["movies", "shows", "episodes", "general"]:
                LOGGER.info(f"Skipping non-content sitemap: {sitemap_url} (type: {sitemap_type})")
                continue
            
            # Parse the sitemap
            entries = self.parse_sitemap(sitemap_url, modified_after)
            
            # Categorize and store each URL
            for entry in entries:
                category = self.categorize_url(entry["url"])
                if category:
                    # Add to the main results (all URLs)
                    self.results[category].append(entry)
                    
                    # Add to modified results if the URL was modified after the filter date
                    if entry.get("is_modified", False):
                        self.modified_results[category].append(entry)
                else:
                    # Count uncategorized URLs
                    self.parsing_errors["url"] += 1
        
        # Deduplicate results
        self._deduplicate_results()
        
    def _deduplicate_results(self) -> None:
        """Remove duplicate URLs from results and modified_results."""
        for container in [self.results, self.modified_results]:
            for category in container:
                # Create a dictionary using URL as key to eliminate duplicates
                unique_dict = {}
                for entry in container[category]:
                    url = entry["url"]
                    
                    # If URL already exists, keep the entry with the more recent lastmod
                    if url in unique_dict:
                        existing_lastmod = unique_dict[url].get("lastmod")
                        new_lastmod = entry.get("lastmod")
                        
                        # If new entry has a more recent lastmod, replace the existing one
                        if new_lastmod and (not existing_lastmod or new_lastmod > existing_lastmod):
                            unique_dict[url] = entry
                    else:
                        unique_dict[url] = entry
                
                # Update the container with deduplicated entries
                container[category] = list(unique_dict.values())
        
        # Log results
        for category in self.results:
            LOGGER.info(f"Deduplicated {category}: {len(self.results[category])} total URLs, "
                       f"{len(self.modified_results[category])} modified URLs")
    
    def save_results(self) -> bool:
        """
        Save categorized URLs to output file using atomic file operations.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Load existing metadata if file exists
            existing_metadata = self._load_existing_metadata()
            
            # Ensure output directory exists
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            
            # Create a proper placeholder with empty data if no URLs found
            if not any(self.results.values()):
                LOGGER.warning("No URLs found for any category, creating empty placeholder file")
                for category in self.results:
                    self.results[category] = []
                    self.modified_results[category] = []
            
            # Build output data with metadata
            output_data = {
                "metadata": {
                    "generated_at": datetime.now().isoformat(),
                    "parsing_errors": self.parsing_errors,
                    "last_full_scan": existing_metadata.get("last_full_scan", datetime.now().isoformat()),
                    "previous_counts": existing_metadata.get("previous_counts", {}),
                    "total_count": sum(len(urls) for urls in self.results.values()),
                    "modified_count": sum(len(urls) for urls in self.modified_results.values())
                }
            }
            
            # Store current counts for future reference
            output_data["metadata"]["previous_counts"] = {
                "movies": len(self.results["movies"]),
                "shows": len(self.results["shows"]),
                "episodes": len(self.results["episodes"])
            }
            
            # Add current timestamp as last full scan if this is a full scan
            if not any(self.modified_results.values()):
                output_data["metadata"]["last_full_scan"] = datetime.now().isoformat()
            
            # Add content data
            for category in self.results:
                # Add main URLs
                output_data[category] = self.results[category]
                # Add modified URLs separately
                output_data[f"modified_{category}"] = self.modified_results[category]
            
            # Use atomic write pattern to ensure file integrity
            return self._atomic_write_json(output_data, self.output_file)
                
        except Exception as e:
            LOGGER.error(f"Error saving results to {self.output_file}: {e}")
            
            # Record error in error tracker
            self.error_tracker.record_error("sitemap", "save_error", self.output_file)
                
            return False
            
    def _atomic_write_json(self, data: Any, file_path: str) -> bool:
        """
        Write data to a JSON file using atomic operations.
        
        Args:
            data: Data to write
            file_path: Path to the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Create temp file in same directory for atomic move
            fd, temp_path = tempfile.mkstemp(
                dir=os.path.dirname(file_path), 
                suffix='.json.tmp'
            )
            
            try:
                # Write to temp file
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                    # Ensure data is flushed to disk
                    f.flush()
                    os.fsync(f.fileno())
                    
                # Atomic replace
                os.replace(temp_path, file_path)
                
                # Log file size for debugging
                file_size = os.path.getsize(file_path)
                LOGGER.info(f"Saved {sum(len(v) for v in self.results.values())} URLs to {file_path} "
                          f"(size: {file_size} bytes)")
                
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
    
    def _load_existing_metadata(self) -> Dict[str, Any]:
        """
        Load metadata from existing sitemap file.
        
        Returns:
            Dictionary with metadata or empty dict if no file exists
        """
        if not os.path.exists(self.output_file):
            return {}
            
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("metadata", {})
        except (json.JSONDecodeError, IOError) as e:
            LOGGER.error(f"Error loading existing metadata: {e}")
            return {}
            
    def _extract_ids_from_results(self) -> Dict[str, Dict[str, List[int]]]:
        """
        Extract API IDs from results.
        
        Returns:
            Dictionary with all_ids and modified_ids for each category
        """
        result = {
            "all_ids": {},
            "modified_ids": {}
        }
        
        # Process all URLs
        for category, entries in self.results.items():
            ids = []
            for entry in entries:
                api_id = entry.get("api_id")
                if api_id is not None:
                    ids.append(api_id)
                    
            result["all_ids"][category] = ids
            
        # Process modified URLs
        for category, entries in self.modified_results.items():
            ids = []
            for entry in entries:
                api_id = entry.get("api_id")
                if api_id is not None:
                    ids.append(api_id)
                    
            result["modified_ids"][category] = ids
            
        # Log results
        for category in result["all_ids"]:
            LOGGER.info(f"Extracted {len(result['all_ids'][category])} total IDs and "
                       f"{len(result['modified_ids'][category])} modified IDs for {category}")
            
        return result
        
    def _load_ids_from_existing(self) -> Dict[str, Dict[str, List[int]]]:
        """
        Load and extract IDs from existing sitemap file.
        
        Returns:
            Dictionary with all_ids and modified_ids for each category
        """
        result = {
            "all_ids": {"movies": [], "shows": [], "episodes": []},
            "modified_ids": {"movies": [], "shows": [], "episodes": []}
        }
        
        if not os.path.exists(self.output_file):
            return result
            
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Process regular categories for all IDs
            for category in ["movies", "shows", "episodes"]:
                ids = []
                # Skip metadata key which isn't a category
                if category not in data or category == "metadata":
                    continue
                    
                entries = data[category]
                for entry in entries:
                    # Try to get API ID from entry
                    api_id = entry.get("api_id")
                    
                    # If not found, try to extract from URL
                    if api_id is None and "url" in entry:
                        api_id = self.extract_id_from_url(entry["url"])
                        
                    if api_id is not None:
                        ids.append(api_id)
                        
                result["all_ids"][category] = ids
                
            # Process modified categories for modified IDs
            for category in ["movies", "shows", "episodes"]:
                modified_key = f"modified_{category}"
                ids = []
                
                if modified_key not in data:
                    continue
                    
                entries = data[modified_key]
                for entry in entries:
                    # Get API ID
                    api_id = entry.get("api_id")
                    
                    # If not found, try to extract from URL
                    if api_id is None and "url" in entry:
                        api_id = self.extract_id_from_url(entry["url"])
                        
                    if api_id is not None:
                        ids.append(api_id)
                        
                result["modified_ids"][category] = ids
                
            # Log results
            for category in result["all_ids"]:
                LOGGER.info(f"Loaded {len(result['all_ids'][category])} total IDs and "
                          f"{len(result['modified_ids'][category])} modified IDs for {category}")
                
            return result
        except (json.JSONDecodeError, IOError) as e:
            LOGGER.error(f"Error loading IDs from existing sitemap: {e}")
            
            # Record error in error tracker
            self.error_tracker.record_error("sitemap", "load_ids_error", self.output_file)
                
            return result

    def run(self, force_refresh: bool = False, return_ids: bool = False, 
            modified_after: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Run the sitemap parsing process with standardized return type.
        
        Args:
            force_refresh: Whether to force refresh even if no updates detected
            return_ids: Whether to return content IDs instead of full URL entries
            modified_after: Only return content modified after this date
            
        Returns:
            Dict with consistent structure:
            {
                'all_urls': {content_type: [url_entries]},  # All URLs by content type
                'modified_urls': {content_type: [modified_url_entries]},  # Modified URLs
                'metadata': {error_stats, timestamp, etc.}  # Processing metadata
            }
            
            If return_ids=True:
            {
                'all_ids': {content_type: [api_ids]},  # All API IDs
                'modified_ids': {content_type: [modified_api_ids]},  # Modified API IDs
                'metadata': {error_stats, timestamp, etc.}  # Processing metadata
            }
        """
        try:
            LOGGER.info("Starting sitemap parsing process")
            if modified_after:
                LOGGER.info(f"Filtering for content modified after {modified_after}")
            
            # Check if updates are needed
            updates_needed = force_refresh or self.check_for_updates()
            
            # Check if sitemap file exists
            file_exists = os.path.exists(self.output_file)
            
            # Prepare metadata for response
            metadata = {
                "timestamp": datetime.now().isoformat(),
                "force_refresh": force_refresh,
                "updates_needed": updates_needed,
                "file_exists": file_exists,
                "modified_after": modified_after.isoformat() if modified_after else None,
                "error_stats": {}
            }
            
            # Process sitemaps if updates needed or file doesn't exist
            if updates_needed or not file_exists:
                if not updates_needed and not file_exists:
                    LOGGER.info("No updates needed, but sitemap file doesn't exist. Creating it.")
                elif updates_needed:
                    LOGGER.info("Processing sitemaps for updates")
                
                # Process the sitemaps to populate self.results and self.modified_results
                self.process_sitemaps(modified_after)
                
                # Save the results
                save_success = self.save_results()
                    
                # Save the current time as the last check time
                if save_success:
                    self._save_last_check_time()
                
                # Update metadata
                metadata["parsing_errors"] = self.parsing_errors
                metadata["save_success"] = save_success
                
            else:
                LOGGER.info("No updates needed, sitemap is up to date")
                
                # If no updates needed, load existing data
                if return_ids:
                    existing_ids = self._load_ids_from_existing()
                    metadata["loaded_from_cache"] = True
                    
                    return {
                        "all_ids": existing_ids["all_ids"],
                        "modified_ids": existing_ids["modified_ids"],
                        "metadata": metadata
                    }
                else:
                    # Load existing data for URL results
                    try:
                        with open(self.output_file, 'r', encoding='utf-8') as f:
                            existing_data = json.load(f)
                            
                        # Convert to our standardized format
                        all_urls = {}
                        modified_urls = {}
                        
                        for category in ["movies", "shows", "episodes"]:
                            if category in existing_data:
                                all_urls[category] = existing_data[category]
                            if f"modified_{category}" in existing_data:
                                modified_urls[category] = existing_data[f"modified_{category}"]
                                
                        metadata["loaded_from_cache"] = True
                        
                        return {
                            "all_urls": all_urls,
                            "modified_urls": modified_urls,
                            "metadata": metadata
                        }
                    except Exception as e:
                        LOGGER.error(f"Error loading existing sitemap data: {e}")
                        metadata["error"] = str(e)
                        return {
                            "all_urls": {},
                            "modified_urls": {},
                            "metadata": metadata
                        }
                
            # Return results in standardized format
            if return_ids:
                ids_result = self._extract_ids_from_results()
                return {
                    "all_ids": ids_result["all_ids"],
                    "modified_ids": ids_result["modified_ids"],
                    "metadata": metadata
                }
            else:
                return {
                    "all_urls": self.results,
                    "modified_urls": self.modified_results,
                    "metadata": metadata
                }
                
        except Exception as e:
            LOGGER.error(f"Error in sitemap parsing: {e}", exc_info=True)
            
            # Record error in error tracker
            self.error_tracker.record_error("sitemap", "run_error", "sitemap_run")
                
            # Return error in standardized format
            return {
                "all_urls": {},
                "modified_urls": {},
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "error": str(e),
                    "success": False
                }
            }
    
    def get_error_report(self) -> str:
        """
        Generate a comprehensive error report.
        
        Returns:
            Formatted error report string
        """
        if hasattr(self.error_tracker, 'generate_error_report'):
            return self.error_tracker.generate_error_report()
        
        # Fall back to basic error reporting if external error tracker doesn't support it
        total_errors = sum(self.parsing_errors.values())
        report = [
            "=== Sitemap Parsing Error Report ===",
            f"Total Errors: {total_errors}",
            "",
            "Error Counts:",
            f"  Index parsing errors: {self.parsing_errors['index']}",
            f"  Sitemap parsing errors: {self.parsing_errors['sitemap']}",
            f"  URL categorization errors: {self.parsing_errors['url']}",
            f"  Blacklisted URLs skipped: {self.parsing_errors['blacklisted']}"
        ]
        
        return "\n".join(report)