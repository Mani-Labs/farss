# File: farsiland_scraper/core/api_client.py
# Version: 4.0.0
# Last Updated: 2025-06-18

"""
Client for WordPress REST API to fetch content from Farsiland.

This module:
1. Connects to WordPress API endpoints
2. Handles pagination and retries
3. Extracts IDs from URLs
4. Maps API responses to scraper item format
5. Supports incremental updates with date filtering
6. Handles rate limiting for API requests
7. Implements memory-efficient LRU cache
8. Provides thread-safe operations

Changelog:
- [4.0.0] Complete overhaul of caching system to prevent memory leaks
- [4.0.0] Implemented LRU cache with proper size limits and expiration
- [4.0.0] Added thread-safe cache operations
- [4.0.0] Improved error recovery with exponential backoff
- [4.0.0] Enhanced session management consistency
- [4.0.0] Added comprehensive cache validation
- [4.0.0] Implemented adaptive memory monitoring
- [4.0.0] Fixed cache entry validation and expiration
- [3.0.1] Fixed improper await usage outside async function
- [3.0.0] Complete rewrite with improved architecture
"""

import os
import requests
import json
import time
import re
import logging
import datetime
import threading
import heapq
from typing import Dict, List, Any, Optional, Union, Generator, Tuple, Set
from urllib.parse import urlparse
from collections import OrderedDict

from farsiland_scraper.config import (
    API_BASE_URL,
    API_TIMEOUT,
    API_RETRIES,
    API_USER_AGENT,
    API_CACHE_ENABLED,
    API_CACHE_TTL,
    LOGGER,
    MAX_MEMORY_MB,
    IN_DOCKER
)

# Try to import psutil for memory monitoring
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    LOGGER.warning("psutil package not available, memory monitoring disabled")

class LRUCache:
    """Thread-safe LRU Cache implementation with size and time expiration."""
    
    def __init__(self, max_size=2000, ttl=3600):
        """
        Initialize the LRU cache.
        
        Args:
            max_size: Maximum number of entries (default: 2000)
            ttl: Time-to-live in seconds (default: 1 hour)
        """
        self.max_size = max_size
        self.ttl = ttl
        self._cache = OrderedDict()
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0
        
    def __len__(self):
        """Return the number of items in the cache."""
        with self._lock:
            return len(self._cache)
            
    def get(self, key, default=None):
        """
        Get an item from the cache.
        
        Args:
            key: Cache key
            default: Default value if key not found
            
        Returns:
            Cached value or default
        """
        with self._lock:
            if key not in self._cache:
                self.misses += 1
                return default
                
            # Check if expired
            entry = self._cache[key]
            if time.time() > entry.get("_expiry", 0):
                self._cache.pop(key)
                self.misses += 1
                return default
                
            # Move to end for LRU behavior
            self._cache.move_to_end(key)
            self.hits += 1
            return entry
            
    def set(self, key, value):
        """
        Set an item in the cache.
        
        Args:
            key: Cache key
            value: Value to store
        """
        with self._lock:
            # Set or update entry
            self._cache[key] = value
            self._cache.move_to_end(key)
            
            # Evict oldest if over capacity
            if len(self._cache) > self.max_size:
                self._cache.popitem(last=False)
                
    def pop(self, key, default=None):
        """
        Remove and return an item from the cache.
        
        Args:
            key: Cache key
            default: Default value if key not found
            
        Returns:
            Cached value or default
        """
        with self._lock:
            return self._cache.pop(key, default)
            
    def clear(self):
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self.hits = 0
            self.misses = 0
            
    def cleanup_expired(self):
        """Remove expired entries from the cache."""
        with self._lock:
            now = time.time()
            expired_keys = [
                k for k, v in self._cache.items() 
                if now > v.get("_expiry", 0)
            ]
            
            for key in expired_keys:
                self._cache.pop(key)
                
            return len(expired_keys)
            
    def get_stats(self):
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache stats
        """
        with self._lock:
            total_calls = self.hits + self.misses
            hit_ratio = (self.hits / total_calls * 100) if total_calls > 0 else 0
            
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "ttl": self.ttl,
                "hits": self.hits,
                "misses": self.misses,
                "hit_ratio": hit_ratio
            }

class APIClient:
    """Client for WordPress REST API with improved memory management and error handling."""
    
    def __init__(self, base_url=None, headers=None, timeout=None, retries=None, 
                 rate_limit_per_minute=60, session_manager=None, error_tracker=None):
        """
        Initialize API client with configuration.
        
        Args:
            base_url: Base URL for the API (default: from config)
            headers: Request headers (default: User-Agent from config)
            timeout: Request timeout in seconds (default: from config)
            retries: Number of retry attempts (default: from config)
            rate_limit_per_minute: Maximum requests per minute
            session_manager: Optional session manager for authentication
            error_tracker: Optional error tracker instance
        """
        self.base_url = base_url or API_BASE_URL
        self.base_url = self.base_url.rstrip('/')
        
        self.headers = headers or {
            "User-Agent": API_USER_AGENT,
            "Accept": "application/json"
        }
        
        self.timeout = timeout or API_TIMEOUT
        self.retries = retries or API_RETRIES
        self.logger = LOGGER
        self.session_manager = session_manager
        self.error_tracker = error_tracker
        
        # Rate limiting
        self.rate_limit = rate_limit_per_minute
        self.request_times = []  # Track request timestamps
        self.request_lock = threading.RLock()  # Thread safety for rate limiting
        
        # Response caching with LRU
        self.cache_enabled = API_CACHE_ENABLED
        self.cache_ttl = API_CACHE_TTL
        
        # Set cache limits based on environment
        if IN_DOCKER or hasattr(os, 'environ') and os.environ.get('FARSILAND_LOW_MEMORY', '').lower() == 'true':
            # More conservative cache for Docker/low-memory environments
            self.max_cache_size = 500  # Maximum number of entries
            self.cache_prune_interval = 300  # 5 minutes
            self.memory_threshold = 0.7  # 70% of MAX_MEMORY_MB
        else:
            # Normal cache for standard environments
            self.max_cache_size = 2000  # Maximum number of entries 
            self.cache_prune_interval = 600  # 10 minutes
            self.memory_threshold = 0.8  # 80% of MAX_MEMORY_MB
        
        # Use LRU cache for response storage
        self.response_cache = LRUCache(max_size=self.max_cache_size, ttl=self.cache_ttl)
        self.failed_ids = set()  # Track consistently failing IDs
        self.last_cache_prune = time.time()
        
        # Create a requests session
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    def _apply_rate_limit(self):
        """
        Apply rate limiting to prevent API throttling.
        Ensures we don't exceed the configured requests per minute.
        """
        with self.request_lock:
            now = time.time()
            
            # Clean up old request timestamps (older than 60 seconds)
            self.request_times = [t for t in self.request_times if now - t < 60]
            
            # If at limit, wait until we can make another request
            if len(self.request_times) >= self.rate_limit:
                oldest = min(self.request_times)
                sleep_time = max(0, 60 - (now - oldest))
                
                if sleep_time > 0:
                    self.logger.debug(f"Rate limit reached. Waiting {sleep_time:.2f}s before next request")
                    time.sleep(sleep_time)
            
            # Record this request
            self.request_times.append(time.time())
        
    def _check_cache(self, key, modified_after=None):
        """
        Check if a cached response exists and is valid.
        
        Args:
            key: Cache key
            modified_after: Only return entries modified after this date
            
        Returns:
            Cached data or None if not found/valid
        """
        if not self.cache_enabled:
            return None
            
        cache_entry = self.response_cache.get(key)
        if not cache_entry:
            return None
            
        # Since cache entry was retrieved, we know it's not expired (LRU handles expiry)
        
        # Check if we should filter by modified_after
        if modified_after and "_modified_date" in cache_entry:
            modified_date = cache_entry["_modified_date"]
            
            # Skip if modified_date is older than modified_after
            if modified_date and modified_date <= modified_after:
                return None
                
        return cache_entry.get("data")
        
    def _store_cache(self, key, data, modified_date=None):
        """
        Store data in cache with timestamp and modified date.
        
        Args:
            key: Cache key
            data: Data to cache
            modified_date: Optional modification date for filtering
        """
        if not self.cache_enabled:
            return
            
        # Create cache entry with metadata
        cache_entry = {
            "data": data,
            "_timestamp": time.time(),
            "_expiry": time.time() + self.cache_ttl,
            "_modified_date": modified_date
        }
        
        # Store in LRU cache
        self.response_cache.set(key, cache_entry)
        
        # Check if it's time to prune expired entries
        current_time = time.time()
        if current_time - self.last_cache_prune > self.cache_prune_interval:
            self._prune_cache()
            self.last_cache_prune = current_time
            
    def _check_memory_usage(self):
        """
        Check current memory usage and prune cache if needed.
        Returns True if cache was pruned, False otherwise.
        """
        if not PSUTIL_AVAILABLE:
            return False
            
        try:
            # Get current process memory usage
            process = psutil.Process()
            memory_info = process.memory_info()
            
            # Calculate memory usage in MB
            memory_usage_mb = memory_info.rss / (1024 * 1024)
            
            # If memory usage exceeds threshold, prune cache
            if memory_usage_mb > (MAX_MEMORY_MB * self.memory_threshold):
                self.logger.warning(f"Memory usage ({memory_usage_mb:.1f}MB) exceeds threshold, pruning cache")
                self._prune_cache(aggressive=True)
                return True
                
        except Exception as e:
            self.logger.warning(f"Error checking memory usage: {e}")
            
        return False
        
    def _prune_cache(self, aggressive=False):
        """
        Clean up cache to prevent memory issues.
        
        Args:
            aggressive: Whether to perform aggressive pruning
        """
        if not self.cache_enabled:
            return
            
        # First, clean up expired entries
        expired_count = self.response_cache.cleanup_expired()
        
        # Get current stats
        stats = self.response_cache.get_stats()
        current_size = stats["size"]
        
        # Log cache stats
        self.logger.info(f"Cache stats: {current_size} entries, {stats['hit_ratio']:.1f}% hit ratio")
        
        # For aggressive pruning, target a smaller size
        target_size = int(self.max_cache_size * 0.5) if aggressive else self.max_cache_size
        
        if current_size <= target_size:
            self.logger.info(f"Removed {expired_count} expired entries from cache")
            return
            
        # If we're still over target size, perform additional pruning
        # This would normally be handled by LRU, but we'll force it for aggressive pruning
        if aggressive and current_size > target_size:
            # Create new cache with smaller max_size
            new_cache = LRUCache(max_size=target_size, ttl=self.cache_ttl)
            
            # Transfer most recent entries from old cache to new cache
            # This will automatically discard older entries due to LRU behavior
            with self.response_cache._lock:
                entries_to_keep = list(self.response_cache._cache.items())[-target_size:]
                for key, value in entries_to_keep:
                    new_cache.set(key, value)
                    
                # Replace old cache with new one
                self.response_cache = new_cache
                
            self.logger.info(f"Aggressively pruned cache from {current_size} to {target_size} entries")
        
    def clear_cache(self):
        """Clear all cached responses to free memory."""
        cache_size = len(self.response_cache)
        self.response_cache.clear()
        self.logger.info(f"Cleared API response cache ({cache_size} entries)")
        
    def _optimize_cache_size(self):
        """Adjust cache size based on available memory and usage patterns."""
        # Always check memory usage first
        if self._check_memory_usage():
            return
            
        # Otherwise, just clean up expired entries
        expired_count = self.response_cache.cleanup_expired()
        if expired_count > 0:
            self.logger.debug(f"Cleaned up {expired_count} expired cache entries")
            
    def get_json(self, endpoint, params=None, modified_after=None):
        """
        Get JSON from endpoint with error handling, retries and rate limiting.
        
        Args:
            endpoint: API endpoint (without base URL)
            params: Query parameters
            modified_after: Only return content modified after this date
            
        Returns:
            Parsed JSON response or None on error
        """
        url = f"{self.base_url}{endpoint}"
        
        # Check if this is a known failing ID
        endpoint_id_match = re.search(r"/(\w+)/(\d+)$", endpoint)
        if endpoint_id_match:
            content_type, content_id = endpoint_id_match.groups()
            content_id = int(content_id)
            
            if content_id in self.failed_ids:
                self.logger.debug(f"Skipping known failing ID: {content_id} ({content_type})")
                return None
            
        # Generate cache key
        params_str = json.dumps(params or {})
        modified_str = modified_after.isoformat() if modified_after else ""
        cache_key = f"{endpoint}:{params_str}:{modified_str}"
        
        # Check cache first
        cached_data = self._check_cache(cache_key, modified_after)
        if cached_data is not None:
            self.logger.debug(f"Using cached response for: {url}")
            return cached_data
        
        # Use authenticated session if available
        session_to_use = self.session
        if self.session_manager:
            try:
                # Use the appropriate method to get the session
                session_to_use = self.session_manager.get_session()
            except Exception as e:
                # Fall back to standard session if we can't get authenticated session
                self.logger.warning(f"Could not get authenticated session: {e}")
                session_to_use = self.session
        
        # Implement retry mechanism with exponential backoff
        for attempt in range(self.retries + 1):
            try:
                # Apply rate limiting
                self._apply_rate_limit()
                
                self.logger.debug(f"GET {url} (attempt {attempt+1}/{self.retries+1})")
                
                # Set appropriate timeout with jitter
                request_timeout = self.timeout * (0.9 + 0.2 * (attempt / self.retries))
                
                response = session_to_use.get(
                    url,
                    params=params,
                    timeout=request_timeout
                )
                
                # Handle 404 errors to track failing IDs
                if response.status_code == 404 and endpoint_id_match:
                    # Extract ID from endpoint
                    content_type, content_id = endpoint_id_match.groups()
                    content_id = int(content_id)
                    
                    self.failed_ids.add(content_id)
                    self.logger.warning(f"Adding ID {content_id} to failed_ids list after 404")
                    
                    # Track in error tracker if available
                    if self.error_tracker:
                        self.error_tracker.record_error(url, f"api_404_{content_type}")
                        
                    return None
                
                # Raise for all other HTTP errors
                response.raise_for_status()
                
                # Validate response format is JSON
                content_type = response.headers.get('Content-Type', '')
                if 'application/json' not in content_type and 'text/json' not in content_type:
                    self.logger.warning(f"Non-JSON response content type: {content_type}")
                
                # Parse JSON response
                data = response.json()
                
                # Extract modified date from response if available
                modified_date = None
                if isinstance(data, dict) and "modified_gmt" in data:
                    try:
                        modified_date = datetime.datetime.fromisoformat(data["modified_gmt"].replace('Z', '+00:00'))
                    except (ValueError, TypeError, AttributeError):
                        pass
                
                # Cache the successful response with modified date
                self._store_cache(cache_key, data, modified_date)
                
                return data
                
            except requests.exceptions.RequestException as e:
                # Different handling based on error type
                if isinstance(e, requests.exceptions.ConnectionError):
                    self.logger.warning(f"Connection error: {e}")
                elif isinstance(e, requests.exceptions.Timeout):
                    self.logger.warning(f"Timeout error: {e}")
                elif isinstance(e, requests.exceptions.HTTPError):
                    self.logger.warning(f"HTTP error {e.response.status_code}: {e}")
                    
                    # Track in error tracker if available
                    if self.error_tracker:
                        self.error_tracker.record_error(url, f"api_http_{e.response.status_code}")
                        
                    # Don't retry client errors (4xx) except 429 (too many requests)
                    if 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                        self.logger.error(f"Client error {e.response.status_code}, not retrying")
                        
                        # Track in error tracker if available
                        if self.error_tracker:
                            self.error_tracker.record_error(url, f"api_client_error_{e.response.status_code}")
                            
                        return None
                else:
                    self.logger.warning(f"Request error: {e}")
                    
                # If this isn't the last attempt, wait before retrying
                if attempt < self.retries:
                    # Exponential backoff with jitter
                    import random
                    base_sleep = 2 ** attempt
                    jitter = random.uniform(0.75, 1.25)
                    sleep_time = base_sleep * jitter
                    
                    # Special handling for rate limiting (429)
                    if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
                        # Try to get retry-after header
                        retry_after = e.response.headers.get('Retry-After')
                        if retry_after:
                            try:
                                sleep_time = int(retry_after) + 1  # Add 1 second buffer
                            except (ValueError, TypeError):
                                # If retry-after header is invalid, use exponential backoff
                                sleep_time = max(sleep_time, 30)  # At least 30 seconds
                        else:
                            # If no retry-after header, use longer backoff
                            sleep_time = max(sleep_time, 30)
                    
                    self.logger.info(f"Retrying in {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                else:
                    self.logger.error(f"Failed after {self.retries} retries")
                    
                    # Track in error tracker if available
                    if self.error_tracker:
                        self.error_tracker.record_error(url, "api_max_retries")
                        
                    return None
                    
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error: {e}")
                
                # Track in error tracker if available
                if self.error_tracker:
                    self.error_tracker.record_error(url, "api_json_decode_error")
                    
                return None
                
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                
                # Track in error tracker if available
                if self.error_tracker:
                    self.error_tracker.record_error(url, "api_unexpected_error")
                    
                return None
                
            finally:
                # Check memory usage after each request
                self._check_memory_usage()
        
        return None
        
    def paginate(self, endpoint, params=None, modified_after=None) -> Generator[List[Dict], None, None]:
        """
        Yield pages of results with pagination handling and rate limiting.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            modified_after: Only return content modified after this date
            
        Yields:
            Each page of results
        """
        params = params or {}
        params.setdefault("per_page", 100)
        
        page = 1
        total_pages = None
        
        while True:
            params["page"] = page
            
            # Generate cache key for this page
            params_str = json.dumps(params)
            modified_str = modified_after.isoformat() if modified_after else ""
            cache_key = f"{endpoint}:{params_str}:{modified_str}"
            
            # Check cache first
            cached_data = self._check_cache(cache_key, modified_after)
            if cached_data is not None:
                self.logger.debug(f"Using cached response for page {page}")
                data = cached_data
            else:
                try:
                    # Apply rate limiting
                    self._apply_rate_limit()
                    
                    # Use authenticated session if available
                    session_to_use = self.session
                    if self.session_manager:
                        try:
                            session_to_use = self.session_manager.get_session()
                        except Exception as e:
                            # Fall back to standard session
                            self.logger.warning(f"Could not get authenticated session: {e}")
                            session_to_use = self.session
                    
                    response = session_to_use.get(
                        f"{self.base_url}{endpoint}",
                        params=params,
                        timeout=self.timeout
                    )
                    
                    if response.status_code != 200:
                        self.logger.error(f"Error {response.status_code} fetching page {page}")
                        
                        # Track in error tracker if available
                        if self.error_tracker:
                            self.error_tracker.record_error(f"{self.base_url}{endpoint}", f"api_http_{response.status_code}")
                            
                        break
                        
                    # Get total pages from headers
                    if total_pages is None and "X-WP-TotalPages" in response.headers:
                        try:
                            total_pages = int(response.headers["X-WP-TotalPages"])
                            self.logger.info(f"Total pages: {total_pages}")
                        except (ValueError, TypeError):
                            total_pages = 1
                    
                    try:
                        data = response.json()
                        
                        # Extract modified date for filtering and caching
                        modified_dates = []
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and "modified_gmt" in item:
                                    try:
                                        modified_date = datetime.datetime.fromisoformat(item["modified_gmt"].replace('Z', '+00:00'))
                                        modified_dates.append(modified_date)
                                    except (ValueError, TypeError, AttributeError):
                                        pass
                        
                        # Use most recent modified date for cache entry
                        cache_modified_date = max(modified_dates) if modified_dates else None
                        
                        # Cache the successful response
                        self._store_cache(cache_key, data, cache_modified_date)
                        
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Error processing page {page}: {e}")
                        
                        # Track in error tracker if available
                        if self.error_tracker:
                            self.error_tracker.record_error(f"{self.base_url}{endpoint}", "api_json_decode_error")
                            
                        break
                        
                except requests.RequestException as e:
                    self.logger.error(f"Request error on page {page}: {e}")
                    
                    # Track in error tracker if available
                    if self.error_tracker:
                        self.error_tracker.record_error(f"{self.base_url}{endpoint}", "api_request_error")
                        
                    break
                    
                finally:
                    # Check memory usage after each page
                    self._check_memory_usage()
            
            # If we got this far, we have data (either from cache or fresh)
            if not data:
                break
                
            # Filter by modified_after if specified
            if modified_after and isinstance(data, list):
                filtered_data = []
                for item in data:
                    if isinstance(item, dict) and "modified_gmt" in item:
                        try:
                            modified_date = datetime.datetime.fromisoformat(item["modified_gmt"].replace('Z', '+00:00'))
                            
                            # Ensure modified_after is timezone-aware if needed
                            if modified_after.tzinfo is None and modified_date.tzinfo is not None:
                                modified_after = modified_after.replace(tzinfo=datetime.timezone.utc)
                                
                            # Include item if it's newer than modified_after
                            if modified_date > modified_after:
                                filtered_data.append(item)
                                
                        except (ValueError, TypeError, AttributeError):
                            # If we can't parse the date, include it to be safe
                            filtered_data.append(item)
                    else:
                        # If no modified_gmt, include it to be safe
                        filtered_data.append(item)
                
                # Use filtered data
                data = filtered_data
            
            # Yield data if any remains after filtering
            if data:
                yield data
            
            # Move to next page
            page += 1
            
            # Check if we've reached the end
            if total_pages and page > total_pages:
                break
            
    def validate_date(self, date_str) -> Optional[datetime.datetime]:
        """
        Validate and format date string for API requests.
        
        Args:
            date_str: Date string in various formats
            
        Returns:
            Datetime object or None if invalid
        """
        if not date_str:
            return None
            
        # Already a datetime object
        if isinstance(date_str, datetime.datetime):
            return date_str
            
        # Try different date formats
        formats = ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d-%m-%Y']
        
        # If string contains 'T' or 'Z', it's likely ISO format
        if 'T' in date_str:
            try:
                return datetime.datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except ValueError:
                pass
        
        # Try standard date formats
        for fmt in formats:
            try:
                dt = datetime.datetime.strptime(date_str, fmt)
                return dt
            except ValueError:
                continue
                
        # Try using dateutil as fallback
        try:
            import dateutil.parser
            return dateutil.parser.parse(date_str)
        except (ImportError, Exception) as e:
            self.logger.error(f"Invalid date format: {date_str} - {e}")
            return None
        
    def get_movie(self, movie_id, modified_after=None):
        """
        Get a single movie by ID.
        
        Args:
            movie_id: WordPress post ID
            modified_after: Only return if modified after this date
            
        Returns:
            Movie data or None on error
        """
        return self.get_json(f"/movies/{movie_id}", {"_embed": 1}, modified_after)
        
    def get_movies(self, page=1, per_page=100, modified_after=None):
        """
        Get a list of movies.
        
        Args:
            page: Page number
            per_page: Items per page
            modified_after: Date string for filtering (ISO format or various formats)
            
        Returns:
            List of movies or None on error
        """
        params = {"page": page, "per_page": per_page, "_embed": 1}
        
        # Add modified_after parameter if provided
        if modified_after:
            # Convert to datetime if it's not already
            modified_date = self.validate_date(modified_after)
            if modified_date:
                # Format for WP API
                params["modified_after"] = modified_date.strftime('%Y-%m-%dT%H:%M:%S')
            
        return self.get_json("/movies", params, modified_after)
        
    def get_show(self, show_id, modified_after=None):
        """
        Get a single TV show by ID.
        
        Args:
            show_id: WordPress post ID
            modified_after: Only return if modified after this date
            
        Returns:
            Show data or None on error
        """
        return self.get_json(f"/tvshows/{show_id}", {"_embed": 1}, modified_after)
        
    def get_shows(self, page=1, per_page=100, modified_after=None):
        """
        Get a list of TV shows.
        
        Args:
            page: Page number
            per_page: Items per page
            modified_after: Date string for filtering (ISO format or various formats)
            
        Returns:
            List of shows or None on error
        """
        params = {"page": page, "per_page": per_page, "_embed": 1}
        
        # Add modified_after parameter if provided
        if modified_after:
            # Convert to datetime if it's not already
            modified_date = self.validate_date(modified_after)
            if modified_date:
                # Format for WP API
                params["modified_after"] = modified_date.strftime('%Y-%m-%dT%H:%M:%S')
            
        return self.get_json("/tvshows", params, modified_after)
        
    def get_episode(self, episode_id, modified_after=None):
        """
        Get a single episode by ID.
        
        Args:
            episode_id: WordPress post ID
            modified_after: Only return if modified after this date
            
        Returns:
            Episode data or None on error
        """
        # Check if this is a known failing ID
        if episode_id in self.failed_ids:
            self.logger.debug(f"Skipping known failing episode ID: {episode_id}")
            return None
            
        return self.get_json(f"/episodes/{episode_id}", {"_embed": 1}, modified_after)
        
    def get_episodes(self, show_id=None, page=1, per_page=100, modified_after=None):
        """
        Get episodes, optionally filtered by show.
        
        Args:
            show_id: Optional TV show ID to filter by
            page: Page number
            per_page: Items per page
            modified_after: Date string for filtering (ISO format or various formats)
            
        Returns:
            List of episodes or None on error
        """
        params = {"page": page, "per_page": per_page, "_embed": 1}
        
        if show_id:
            params["tvshows"] = show_id
            
        # Add modified_after parameter if provided
        if modified_after:
            # Convert to datetime if it's not already
            modified_date = self.validate_date(modified_after)
            if modified_date:
                # Format for WP API
                params["modified_after"] = modified_date.strftime('%Y-%m-%dT%H:%M:%S')
            
        return self.get_json("/episodes", params, modified_after)
        
    def get_modified_content(self, content_type, since_date, per_page=100):
        """
        Get all content modified since a specific date.
        
        Args:
            content_type: Content type ('movies', 'tvshows', 'episodes')
            since_date: Date string (various formats)
            per_page: Items per page
            
        Returns:
            List of modified content items
        """
        # Map content types to endpoints
        endpoint_map = {
            "movies": "/movies",
            "shows": "/tvshows",
            "tvshows": "/tvshows",
            "episodes": "/episodes"
        }
        
        endpoint = endpoint_map.get(content_type)
        if not endpoint:
            self.logger.error(f"Unknown content type: {content_type}")
            return []
            
        # Validate date
        modified_date = self.validate_date(since_date)
        if not modified_date:
            self.logger.error(f"Invalid date format: {since_date}")
            return []
            
        all_items = []
        for page in self.paginate(endpoint, {
            "per_page": per_page,
            "modified_after": modified_date.strftime('%Y-%m-%dT%H:%M:%S'),
            "_embed": 1
        }, modified_date):
            all_items.extend(page)
            
        return all_items
        
    def get_all_modified_since(self, since_date, batch_size=100, 
                               content_types=None) -> Dict[str, List[Dict]]:
        """
        Get all content modified since a date across multiple content types.
        Efficiently handles batching and combines results.
        
        Args:
            since_date: Date string (various formats)
            batch_size: Number of items per batch request
            content_types: List of content types to check, or None for all
            
        Returns:
            Dictionary mapping content types to lists of modified items
        """
        if content_types is None:
            content_types = ["movies", "shows", "episodes"]
            
        # Validate date
        modified_date = self.validate_date(since_date)
        if not modified_date:
            self.logger.error(f"Invalid date format: {since_date}")
            return {ct: [] for ct in content_types}
            
        results = {}
        
        for content_type in content_types:
            self.logger.info(f"Fetching modified {content_type} since {modified_date}")
            modified_items = self.get_modified_content(content_type, modified_date, batch_size)
            results[content_type] = modified_items
            self.logger.info(f"Found {len(modified_items)} modified {content_type}")
            
            # Check memory usage after processing each content type
            self._check_memory_usage()
            
        return results
        
    def process_batch_ids(self, content_type, ids, batch_size=10) -> List[Dict]:
        """
        Process a batch of IDs efficiently.
        
        Args:
            content_type: Content type ('movies', 'shows', 'episodes')
            ids: List of IDs to fetch
            batch_size: Number of IDs to process in each batch
            
        Returns:
            List of fetched items
        """
        # Map content types to endpoints
        endpoint_map = {
            "movies": "/movies",
            "shows": "/tvshows",
            "tvshows": "/tvshows",
            "episodes": "/episodes"
        }
        
        endpoint = endpoint_map.get(content_type)
        if not endpoint:
            self.logger.error(f"Unknown content type: {content_type}")
            return []
            
        # Filter out known failing IDs
        if content_type == "episodes":
            original_count = len(ids)
            ids = [id for id in ids if id not in self.failed_ids]
            if len(ids) < original_count:
                self.logger.info(f"Filtered out {original_count - len(ids)} known failing episode IDs")
            
        results = []
        total_ids = len(ids)
        
        # Process IDs in batches
        for i in range(0, total_ids, batch_size):
            batch = ids[i:i+batch_size]
            self.logger.info(f"Processing {content_type} batch {i//batch_size + 1}/{(total_ids + batch_size - 1)//batch_size} ({len(batch)} items)")
            
            # WordPress REST API supports include parameter for multiple IDs
            params = {
                "include": ",".join(str(id) for id in batch),
                "per_page": batch_size,
                "_embed": 1
            }
            
            items = self.get_json(endpoint, params)
            if items:
                results.extend(items)
                
            # Check memory usage after each batch
            self._check_memory_usage()
                
        return results
        
    def extract_id_from_url(self, url):
        """
        Extract WordPress post ID from URL.
        
        Args:
            url: URL to extract from
            
        Returns:
            Post ID or None
        """
        if not url:
            return None
            
        # Try query parameter
        if "?p=" in url:
            try:
                match = re.search(r"[?&]p=(\d+)", url)
                if match:
                    return int(match.group(1))
            except (ValueError, TypeError):
                pass
                
        # Try post ID in slug
        try:
            # Pattern like /post-name-12345/ or /12345/
            match = re.search(r"/(\d+)/?$", url)
            if match:
                return int(match.group(1))
                
            # Pattern like /post-name-12345-another-word/
            match = re.search(r"-(\d+)-", url)
            if match:
                return int(match.group(1))
        except (ValueError, TypeError):
            pass
            
        return None

    def _validate_api_response(self, api_data: dict, item_type: str) -> bool:
        """
        Validate if API response has all required fields.
        
        Args:
            api_data: API response data
            item_type: Type of content ('movies', 'shows', 'episodes')
            
        Returns:
            True if response is valid and complete, False otherwise
        """
        if not api_data or not isinstance(api_data, dict):
            return False
            
        # Check common required fields
        if 'id' not in api_data or 'link' not in api_data:
            return False
            
        # Content-specific validation
        if item_type in ["shows", "tvshows"]:
            # Show must have title and acf field with basic data
            if not api_data.get('title', {}).get('rendered'):
                return False
                
            # ACF fields should exist and contain basic show info
            acf = api_data.get('acf', {})
            if not acf or not any([
                acf.get('title_fa'),
                acf.get('season_count'),
                acf.get('episode_count')
            ]):
                return False
                
        return True    
    
    def map_api_to_item(self, api_data, item_type, modified_after=None):
        """
        Map API data to scraper item format.
        
        Args:
            api_data: Data from API
            item_type: Type of item ('movies', 'shows', 'episodes')
            modified_after: Optional date filter
            
        Returns:
            Mapped item dict or None if filtered out or invalid
        """
        if not api_data:
            return None
            
        # Validate API response completeness
        if not self._validate_api_response(api_data, item_type):
            self.logger.warning(f"Incomplete API data for {item_type} ID {api_data.get('id')}")
            return None
            
        # Check if this item should be filtered by date
        if modified_after and "modified_gmt" in api_data:
            try:
                # Convert string to datetime
                modified_date = datetime.datetime.fromisoformat(api_data["modified_gmt"].replace('Z', '+00:00'))
                
                # Convert modified_after to datetime if it's not already
                if isinstance(modified_after, str):
                    modified_after = self.validate_date(modified_after)
                
                # Make modified_after timezone-aware if it isn't already
                if modified_after and modified_after.tzinfo is None:
                    modified_after = modified_after.replace(tzinfo=datetime.timezone.utc)
                    
                # Skip if older than modified_after
                if modified_after and modified_date <= modified_after:
                    return None
            except (ValueError, TypeError, AttributeError):
                # If we can't parse the date, include it to be safe
                pass
        
        # Common fields
        item = {
            "api_id": api_data.get("id"),
            "api_source": "wp-json/v2",
            "modified_gmt": api_data.get("modified_gmt"),
            "url": api_data.get("link"),
            "description": api_data.get("content", {}).get("rendered", ""),
            "is_new": 1  # Mark as new by default
        }
        
        # Handle title field differently based on content type
        # Episodes use "title" while shows and movies use "title_en"
        title_value = api_data.get("title", {}).get("rendered", "")
        if item_type == "episodes":
            item["title"] = title_value
        else:
            item["title_en"] = title_value
        
        # Set is_modified flag for incremental updates
        if modified_after:
            item["is_modified"] = True
        
        # Media/poster - Use correct field name based on item type
        if "_embedded" in api_data and "wp:featuredmedia" in api_data["_embedded"]:
            media = api_data["_embedded"]["wp:featuredmedia"]
            if media and isinstance(media, list) and len(media) > 0:
                if item_type == "episodes":
                    item["thumbnail"] = media[0].get("source_url")
                else:
                    item["poster"] = media[0].get("source_url")
        
        # Type-specific fields
        if item_type == "movies":
            # Process movie fields
            self._map_movie_fields(api_data, item)
        elif item_type in ["shows", "tvshows"]:
            # Process show fields
            self._map_show_fields(api_data, item)
        elif item_type == "episodes":
            # Process episode fields
            self._map_episode_fields(api_data, item)
            
        # Ensure poster is mapped to thumbnail for episodes
        if item_type == "episodes" and "poster" in item:
            item["thumbnail"] = item.pop("poster")
            
        return item
    
    def _map_movie_fields(self, api_data, item):
        """Map movie-specific fields from API data."""
        # Extract custom fields
        acf = api_data.get("acf", {})
        
        # Title in Farsi if available
        if "title_fa" in acf:
            item["title_fa"] = acf.get("title_fa")
            
        # Release date and year
        if "release_date" in acf:
            item["release_date"] = acf.get("release_date")
            # Try to extract year
            if item["release_date"]:
                year_match = re.search(r"(\d{4})", item["release_date"])
                if year_match:
                    item["year"] = int(year_match.group(1))
                    
        # Rating information
        if "rating" in acf:
            try:
                item["rating"] = float(acf.get("rating", 0))
            except (ValueError, TypeError):
                item["rating"] = 0.0
                
        if "rating_count" in acf:
            try:
                item["rating_count"] = int(acf.get("rating_count", 0))
            except (ValueError, TypeError):
                item["rating_count"] = 0
                
        # Extract genres, directors, cast from taxonomy terms
        if "_embedded" in api_data and "wp:term" in api_data["_embedded"]:
            terms = api_data["_embedded"]["wp:term"]
            item["genres"] = []
            item["directors"] = []
            item["cast"] = []
            
            for term_list in terms:
                for term in term_list:
                    tax = term.get("taxonomy")
                    if tax == "genres":
                        item["genres"].append(term.get("name"))
                    elif tax == "director":
                        item["directors"].append(term.get("name"))
                    elif tax == "cast":
                        item["cast"].append(term.get("name"))
        
        # Video files if available
        if "video_files" in acf:
            try:
                item["video_files"] = acf.get("video_files", [])
            except:
                item["video_files"] = []
                
    def _map_show_fields(self, api_data, item):
        """Map show-specific fields from API data."""
        # Extract custom fields
        acf = api_data.get("acf", {})
        
        # Title in Farsi if available
        if "title_fa" in acf:
            item["title_fa"] = acf.get("title_fa")
            
        # First air date
        if "first_air_date" in acf:
            item["first_air_date"] = acf.get("first_air_date")
            
        # Last aired date
        if "last_aired" in acf:
            item["last_aired"] = acf.get("last_aired")
            
        # Season and episode counts
        if "season_count" in acf:
            try:
                item["season_count"] = int(acf.get("season_count", 0))
            except (ValueError, TypeError):
                item["season_count"] = 0
                
        if "episode_count" in acf:
            try:
                item["episode_count"] = int(acf.get("episode_count", 0))
            except (ValueError, TypeError):
                item["episode_count"] = 0
                
        # Rating information
        if "rating" in acf:
            try:
                item["rating"] = float(acf.get("rating", 0))
            except (ValueError, TypeError):
                item["rating"] = 0.0
                
        if "rating_count" in acf:
            try:
                item["rating_count"] = int(acf.get("rating_count", 0))
            except (ValueError, TypeError):
                item["rating_count"] = 0
                
        # Extract genres, directors, cast from taxonomy terms
        if "_embedded" in api_data and "wp:term" in api_data["_embedded"]:
            terms = api_data["_embedded"]["wp:term"]
            item["genres"] = []
            item["directors"] = []
            item["cast"] = []
            
            for term_list in terms:
                for term in term_list:
                    tax = term.get("taxonomy")
                    if tax == "genres":
                        item["genres"].append(term.get("name"))
                    elif tax == "director":
                        item["directors"].append(term.get("name"))
                    elif tax == "cast":
                        item["cast"].append(term.get("name"))
        
        # Seasons data if available
        if "seasons" in acf:
            try:
                item["seasons"] = acf.get("seasons", [])
            except:
                item["seasons"] = []
    
    def _map_episode_fields(self, api_data, item):
        """Map episode-specific fields from API data."""
        # Extract custom fields
        acf = api_data.get("acf", {})
        
        # Episode numbering
        if "season_number" in acf:
            try:
                item["season_number"] = int(acf.get("season_number", 1))
            except (ValueError, TypeError):
                item["season_number"] = 1
                
        if "episode_number" in acf:
            try:
                item["episode_number"] = int(acf.get("episode_number", 1))
            except (ValueError, TypeError):
                item["episode_number"] = 1
                
        # Air date
        if "air_date" in acf:
            item["air_date"] = acf.get("air_date")
            
        # Thumbnail
        if "thumbnail" in acf:
            item["thumbnail"] = acf.get("thumbnail")
            
        # Video files if available
        if "video_files" in acf:
            try:
                item["video_files"] = acf.get("video_files", [])
            except:
                item["video_files"] = []
                
        # Get show information
        if "_embedded" in api_data and "wp:term" in api_data["_embedded"]:
            terms = api_data["_embedded"]["wp:term"]
            for term_list in terms:
                for term in term_list:
                    if term.get("taxonomy") == "tvshows":
                        item["show_id"] = term.get("id")
                        item["show_url"] = term.get("link")
                        
    def get_stats(self):
        """
        Get comprehensive statistics about API client usage.
        
        Returns:
            Dictionary with API client stats
        """
        # Get cache stats
        cache_stats = self.response_cache.get_stats()
        
        # Memory usage if available
        memory_usage = None
        if PSUTIL_AVAILABLE:
            try:
                process = psutil.Process()
                memory_info = process.memory_info()
                memory_usage = {
                    "rss_mb": memory_info.rss / (1024 * 1024),
                    "vms_mb": memory_info.vms / (1024 * 1024),
                    "percent": process.memory_percent()
                }
            except Exception as e:
                self.logger.warning(f"Could not get memory stats: {e}")
                
        return {
            "cache": cache_stats,
            "memory": memory_usage,
            "failed_ids_count": len(self.failed_ids),
            "timestamp": datetime.datetime.now().isoformat()
        }
                        
    def close(self):
        """Close the session and clean up resources."""
        try:
            # Save cache stats
            stats = self.response_cache.get_stats()
            hit_ratio = stats["hit_ratio"]
            self.logger.info(f"Final cache stats: {stats['size']} entries, {hit_ratio:.1f}% hit ratio")
            
            # Close session
            if self.session:
                self.session.close()
                
            # Clear cache
            self.clear_cache()
        except Exception as e:
            self.logger.error(f"Error closing API client: {e}")
            
    def __del__(self):
        """Clean up resources when object is garbage collected."""
        self.close()