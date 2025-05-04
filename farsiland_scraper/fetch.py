# File: farsiland_scraper/fetch.py
# Version: 2.1.0
# Last Updated: 2025-06-15

"""
Utility functions for fetching and caching web pages.

Features:
- Platform-independent file locking
- Memory-efficient content handling
- Support for proxies and authentication
- Proper handling of Persian/Arabic URLs
- Timeout configuration for network operations
- Integration with error tracking system
- Content type validation
- Size limits for downloads
- Support for headless environments

Changelog:
- [2.1.0] Fixed memory management for large responses
- [2.1.0] Improved cache invalidation strategy
- [2.1.0] Fixed character encoding handling issues
- [2.1.0] Improved thread safety for file operations
- [2.1.0] Fixed potential deadlocks in resource management
- [2.1.0] Added better error recovery for network failures
- [2.1.0] Fixed proxy authentication handling
- [2.0.0] Added cross-platform file locking
- [2.0.0] Improved URL handling for non-ASCII characters
- [2.0.0] Added proxy and authentication support
- [2.0.0] Fixed memory management for large responses
- [2.0.0] Added session management integration
- [2.0.0] Improved error handling and recovery
- [2.0.0] Added CAPTCHA detection
- [1.2.0] Added content type validation
- [1.2.0] Added size limit for downloads
"""

import os
import re
import hashlib
import aiohttp
import logging
import asyncio
import random
import traceback
import unicodedata
import contextlib
from pathlib import Path
from typing import Optional, Dict, Any, Union, List, Callable
from datetime import datetime
from urllib.parse import quote, unquote, urlparse, urlunparse

from farsiland_scraper.config import (
    CACHE_DIR, 
    LOGGER, 
    REQUEST_TIMEOUT, 
    REQUEST_RETRY_COUNT, 
    REQUEST_RETRY_DELAY,
    MAX_CONTENT_SIZE,
    PROXY_URL,
    PROXY_AUTH,
    IN_DOCKER
)

# Cache directory
CACHE_BASE = Path(CACHE_DIR) / "pages"
CACHE_BASE.mkdir(parents=True, exist_ok=True)

class CaptchaDetectedException(Exception):
    """Exception raised when a CAPTCHA is detected."""
    def __init__(self, url: str, message: str = "CAPTCHA detected"):
        self.url = url
        self.message = message
        super().__init__(f"{message} on {url}")


# Platform-specific file locking with more robust error handling
if os.name == 'nt':  # Windows
    import msvcrt
    
    def lock_file(f):
        """
        Lock a file on Windows with improved error handling.
        
        Args:
            f: File descriptor
            
        Returns:
            True if lock acquired, False otherwise
        """
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except (IOError, PermissionError, OSError) as e:
            LOGGER.debug(f"Windows file locking error: {e}")
            return False
        
    def unlock_file(f):
        """
        Unlock a file on Windows with improved error handling.
        
        Args:
            f: File descriptor
            
        Returns:
            True if unlocked, False otherwise
        """
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
            return True
        except (IOError, PermissionError, OSError) as e:
            LOGGER.debug(f"Windows file unlocking error: {e}")
            return False
else:  # Unix
    import fcntl
    
    def lock_file(f):
        """
        Lock a file on Unix with improved error handling.
        
        Args:
            f: File descriptor
            
        Returns:
            True if lock acquired, False otherwise
        """
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (IOError, PermissionError, OSError) as e:
            LOGGER.debug(f"Unix file locking error: {e}")
            return False
        
    def unlock_file(f):
        """
        Unlock a file on Unix with improved error handling.
        
        Args:
            f: File descriptor
            
        Returns:
            True if unlocked, False otherwise
        """
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            return True
        except (IOError, PermissionError, OSError) as e:
            LOGGER.debug(f"Unix file unlocking error: {e}")
            return False


def normalize_text(text: str) -> str:
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
    try:
        text = unicodedata.normalize('NFKC', text)
    except (TypeError, UnicodeError) as e:
        LOGGER.warning(f"Unicode normalization error: {e}")
        # Fall back to non-normalized text
        return text
    
    # Convert Persian/Arabic digits to Latin
    try:
        persian_digits = {
            '۰': '0', '۱': '1', '۲': '2', '۳': '3', '۴': '4',
            '۵': '5', '۶': '6', '۷': '7', '۸': '8', '۹': '9'
        }
        for persian, latin in persian_digits.items():
            text = text.replace(persian, latin)
    except (TypeError, UnicodeError) as e:
        LOGGER.warning(f"Persian digit conversion error: {e}")
        
    return text


def slugify_url(url: str) -> str:
    """
    Convert a URL to a safe filename with proper handling of Unicode.
    
    Args:
        url: The URL to convert
        
    Returns:
        A safe filename generated from the URL
    """
    if not url:
        return "empty_url"
        
    try:
        # First decode any percent-encoded components
        try:
            url = unquote(url)
        except Exception as e:
            LOGGER.debug(f"URL unquote failed for {url}: {e}")
            # If decoding fails, use original
            pass
        
        # Normalize URL text (handle Persian/Arabic characters)
        url = normalize_text(url)
        
        # Create an MD5 hash of the normalized URL
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:12]
        
        # Extract domain and path for human-readable part
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.split(':')[0]  # Remove port if present
            path = parsed.path.rstrip('/').lstrip('/')
            
            # Keep only alphanumeric characters and replace others with underscore
            domain = re.sub(r'[^a-zA-Z0-9]', '_', domain)
            path = re.sub(r'[^a-zA-Z0-9]', '_', path)
            
            # Limit length
            domain = domain[:20]
            path = path[:30]
            
            # Combine components
            if path:
                slug = f"{domain}_{path}_{url_hash}"
            else:
                slug = f"{domain}_{url_hash}"
            
            # Remove duplicate underscores
            slug = re.sub(r'_+', '_', slug)
            
            return slug
        except Exception as e:
            LOGGER.debug(f"URL parsing failed, using hash only: {e}")
            # Fallback to just the hash if parsing fails
            return f"url_{url_hash}"
    except Exception as e:
        LOGGER.warning(f"Slugify URL failed completely for: {url}, {e}")
        # Ultimate fallback for any catastrophic failure
        return f"url_{datetime.now().timestamp()}"


def get_cache_path(url: str, content_type: str) -> Path:
    """
    Get the cache file path for a URL.
    
    Args:
        url: The URL to cache
        content_type: The type of content (e.g., 'movies', 'episodes')
        
    Returns:
        The path to the cache file
    """
    # Generate safe filename from URL
    filename = f"{slugify_url(url)}.html"
    
    # Create content type subdirectory if it doesn't exist
    cache_dir = CACHE_BASE / content_type
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    return cache_dir / filename


def _contains_captcha(html: str) -> bool:
    """
    Check if HTML content contains a CAPTCHA.
    
    Args:
        html: HTML content to check
        
    Returns:
        True if CAPTCHA detected, False otherwise
    """
    if not html:
        return False
        
    # Common CAPTCHA indicators
    captcha_patterns = [
        'captcha',
        'recaptcha',
        'g-recaptcha',
        'hcaptcha',
        'captchaContainer',
        'verification required',
        'verify you are human',
        'security check',
        'bot protection'
    ]
    
    try:
        html_lower = html.lower()
        return any(pattern in html_lower for pattern in captcha_patterns)
    except (AttributeError, TypeError) as e:
        LOGGER.warning(f"Error checking for CAPTCHA: {e}")
        return False


async def create_session(proxy: Optional[str] = None, auth=None) -> aiohttp.ClientSession:
    """
    Create an aiohttp ClientSession with optional proxy and auth.
    
    Args:
        proxy: Optional proxy URL
        auth: Optional aiohttp.BasicAuth for authentication
        
    Returns:
        Configured aiohttp.ClientSession
    """
    # Use default proxy if none specified
    if proxy is None:
        proxy = PROXY_URL
        
    # Create session with appropriate configuration
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5,fa;q=0.3"  # Added Farsi language support
    }
    
    # Default TCPConnector with improved settings
    connector = aiohttp.TCPConnector(
        ssl=False,
        force_close=False,
        enable_cleanup_closed=True,
        limit=20,  # Maximum number of simultaneous connections
        ttl_dns_cache=300,  # DNS cache TTL in seconds
    )
    
    session = aiohttp.ClientSession(headers=headers, connector=connector)
    
    # Configure session with proxy if provided
    if proxy:
        try:
            # Setup proxy auth if credentials available
            proxy_auth = None
            if PROXY_AUTH['username'] and PROXY_AUTH['password']:
                proxy_auth = aiohttp.BasicAuth(
                    PROXY_AUTH['username'],
                    PROXY_AUTH['password']
                )
                
            # Update session with proxy settings by updating connector
            connector._proxy = proxy
            connector._proxy_auth = proxy_auth
        except Exception as e:
            LOGGER.warning(f"Error setting up proxy: {e}")
            
    return session


async def fetch_and_cache(
    url: str, 
    content_type: str, 
    lastmod: Optional[str] = None, 
    force_refresh: bool = False,
    timeout: Optional[int] = None,
    retries: Optional[int] = None,
    max_size: Optional[int] = None,
    proxy: Optional[str] = None,
    session: Optional[aiohttp.ClientSession] = None,
    session_manager = None,
    error_tracker = None,
    modified_after: Optional[datetime] = None
) -> Optional[str]:
    """
    Fetch a URL and cache the result.
    
    Args:
        url: The URL to fetch
        content_type: The type of content
        lastmod: Last modification timestamp from sitemap
        force_refresh: Whether to force a refresh regardless of cache
        timeout: Request timeout in seconds (defaults to config value)
        retries: Number of retry attempts (defaults to config value)
        max_size: Maximum content size to download (defaults to config value)
        proxy: Optional proxy URL (e.g., 'http://user:pass@some.proxy.com:8080/')
        session: Optional existing aiohttp session to reuse
        session_manager: Optional session manager for authenticated requests
        error_tracker: Optional error tracker to record issues
        modified_after: Only fetch if modified after this date
        
    Returns:
        The HTML content or None if fetching failed
    """
    # Use config values if not specified
    if timeout is None:
        timeout = REQUEST_TIMEOUT
    if retries is None:
        retries = REQUEST_RETRY_COUNT
    if max_size is None:
        max_size = MAX_CONTENT_SIZE
    
    # Clean and normalize the URL
    url = url.strip()
    
    # Create paths
    try:
        cache_path = get_cache_path(url, content_type)
        cache_dir = cache_path.parent
        os.makedirs(cache_dir, exist_ok=True)
    except Exception as e:
        LOGGER.error(f"Error creating cache directory: {e}")
        # Fall back to a simpler path if needed
        cache_path = CACHE_BASE / f"{hashlib.md5(url.encode()).hexdigest()}.html"
        cache_dir = CACHE_BASE
        os.makedirs(cache_dir, exist_ok=True)
    
    # Skip if URL is blacklisted by error tracker
    if error_tracker and error_tracker.should_skip(url):
        LOGGER.info(f"Skipping blacklisted URL: {url}")
        return None
    
    try:
        # Check if cache exists and is valid
        if cache_path.exists() and not force_refresh:
            try:
                cache_mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
                
                # If we have a modified_after date and the cache is newer than that date,
                # we can use the cached version
                if modified_after and cache_mtime > modified_after:
                    LOGGER.info(f"Using cached version of {url} (cache newer than filter date)")
                    try:
                        with open(cache_path, 'r', encoding='utf-8') as f:
                            return f.read()
                    except UnicodeDecodeError:
                        # If UTF-8 fails, try with different encoding
                        with open(cache_path, 'r', encoding='latin-1') as f:
                            return f.read()
                    
                # Original lastmod-based cache validation
                if lastmod:
                    try:
                        lastmod_time = datetime.fromisoformat(lastmod.replace('Z', '+00:00'))
                        if cache_mtime >= lastmod_time:
                            LOGGER.info(f"Using cached version of {url}")
                            with open(cache_path, 'r', encoding='utf-8') as f:
                                return f.read()
                    except (ValueError, TypeError) as e:
                        LOGGER.warning(f"Could not compare lastmod for {url}: {e}")
                        with open(cache_path, 'r', encoding='utf-8') as f:
                            return f.read()
                else:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        return f.read()
            except (OSError, IOError) as e:
                LOGGER.warning(f"Error reading cache file for {url}: {e}")
                # Continue to fetch the URL if cache read fails

        # If cache doesn't exist or is invalid, fetch the URL
        should_close_session = False
        try:
            if session is None:
                # Try to get authenticated session from session manager
                if session_manager:
                    try:
                        # Use the async method correctly
                        session = await session_manager.ensure_session()
                    except Exception as e:
                        LOGGER.warning(f"Could not get authenticated session: {e}")
                        # Fall back to creating a new unauthenticated session
                        session = await create_session(proxy)
                else:
                    # Create a standard session
                    session = await create_session(proxy)
                    
                should_close_session = True
            
            html = None  # Initialize html variable
            
            for attempt in range(retries):
                try:
                    # Add headers to limit size and set accepted content types
                    headers = {
                        "Range": f"bytes=0-{max_size}",
                        "Accept": "text/html,application/xhtml+xml",
                        "Accept-Language": "en-US,en;q=0.5,fa;q=0.3"  # Added Farsi language support
                    }
                    
                    # Use a timeout context
                    timeout_ctx = aiohttp.ClientTimeout(total=timeout)
                    
                    async with session.get(url, timeout=timeout_ctx, headers=headers) as response:
                        if response.status == 200 or response.status == 206:  # 206 = Partial Content
                            # Validate content type
                            content_type_header = response.headers.get('Content-Type', '')
                            is_html = (
                                content_type_header.startswith('text/html') or 
                                content_type_header.startswith('application/xhtml+xml')
                            )
                            
                            if not is_html:
                                LOGGER.warning(f"Unexpected content type: {content_type_header} for {url}")
                                # Continue anyway, might still be useful
                            
                            # Check content length
                            if 'Content-Length' in response.headers:
                                try:
                                    length = int(response.headers['Content-Length'])
                                    if length > max_size:
                                        LOGGER.warning(f"Content size exceeds limit: {length} bytes for {url}")
                                        # Continue with range request
                                except (ValueError, TypeError):
                                    pass
                            
                            # Read content in chunks to manage memory better
                            chunks = []
                            total_size = 0
                            chunk_size = 8192  # 8KB chunks
                            
                            async for chunk in response.content.iter_chunked(chunk_size):
                                chunks.append(chunk)
                                total_size += len(chunk)
                                
                                # Check if we've exceeded max size
                                if total_size > max_size:
                                    LOGGER.warning(f"Stopped reading after reaching size limit: {total_size} bytes")
                                    break
                            
                            # Combine chunks and decode
                            content = b''.join(chunks)
                            try:
                                html = content.decode('utf-8')
                            except UnicodeDecodeError:
                                # Fall back to a more lenient encoding if UTF-8 fails
                                html = content.decode('latin-1')
                            
                            # Check for CAPTCHA
                            if _contains_captcha(html):
                                LOGGER.error(f"CAPTCHA detected on {url}")
                                
                                # Record error if tracker available
                                if error_tracker:
                                    error_tracker.record_error(url, "captcha_detected")
                                    
                                raise CaptchaDetectedException(url)
                            
                            # Write to cache using context manager for better resource handling
                            try:
                                with open(cache_path, 'w', encoding='utf-8') as f:
                                    # Try to acquire lock
                                    if lock_file(f):
                                        f.write(html)
                                        
                                        # Explicit flush to ensure data is written
                                        f.flush()
                                        os.fsync(f.fileno())
                                        
                                        # Release lock before closing
                                        unlock_file(f)
                                    else:
                                        LOGGER.warning(f"Could not acquire lock for {url}, skipping cache write")
                                    
                                LOGGER.info(f"Fetched and cached {url}")
                            except (IOError, OSError) as e:
                                LOGGER.error(f"Error writing cache file for {url}: {e}")
                                # Continue anyway - we still have the fetched content
                                
                            return html
                        else:
                            LOGGER.warning(f"Failed to fetch {url} (status: {response.status})")
                            
                            # Record error if tracker available
                            if error_tracker:
                                error_tracker.record_error(url, f"http_{response.status}")
                            
                            # Don't retry for client errors (4xx) except 429 (too many requests)
                            if 400 <= response.status < 500 and response.status != 429:
                                break
                except CaptchaDetectedException:
                    # Re-raise CAPTCHA exceptions 
                    raise
                except aiohttp.ClientError as e:
                    if isinstance(e, aiohttp.ServerTimeoutError):
                        LOGGER.warning(f"Timeout fetching {url} (attempt {attempt+1}/{retries})")
                    elif isinstance(e, aiohttp.ClientConnectorError):
                        LOGGER.warning(f"Connection error for {url}: {e}")
                    else:
                        LOGGER.warning(f"Client error fetching {url}: {e}")
                        
                    # Record error if tracker available
                    if error_tracker:
                        error_tracker.record_error(url, "http_client_error")
                except asyncio.TimeoutError:
                    LOGGER.warning(f"Asyncio timeout fetching {url} (attempt {attempt+1}/{retries})")
                    
                    # Record error if tracker available
                    if error_tracker:
                        error_tracker.record_error(url, "asyncio_timeout")
                except Exception as e:
                    LOGGER.error(f"[FETCH FAIL] Error fetching {url}: {e}")
                    LOGGER.debug(traceback.format_exc())
                    
                    # Record error if tracker available
                    if error_tracker:
                        error_tracker.record_error(url, "fetch_error")
                
                # If we're not on the last attempt, wait before retrying
                if attempt < retries - 1:
                    # Implement proper exponential backoff with jitter
                    backoff_time = REQUEST_RETRY_DELAY * (2 ** attempt)
                    jitter = random.uniform(0.75, 1.25)  # ±25% jitter
                    sleep_time = backoff_time * jitter
                    
                    LOGGER.info(f"Retrying in {sleep_time:.1f}s (attempt {attempt+1}/{retries})")
                    await asyncio.sleep(sleep_time)
            
            # If we get here, all retry attempts failed
            return None
            
        finally:
            # Clean up session if we created it
            if should_close_session and session:
                try:
                    await session.close()
                except Exception:
                    pass
    except CaptchaDetectedException:
        # Let CAPTCHA exceptions propagate
        raise
    except Exception as e:
        LOGGER.error(f"Unexpected error in fetch_and_cache for {url}: {e}")
        
        # Record error if tracker available
        if error_tracker:
            error_tracker.record_error(url, "unexpected_error")
            
        return None
        
# Synchronous wrapper for use in blocking code
def fetch_sync(
    url: str, 
    content_type: str, 
    lastmod: Optional[str] = None, 
    force_refresh: bool = False,
    timeout: Optional[int] = None,
    retries: Optional[int] = None,
    max_size: Optional[int] = None,
    proxy: Optional[str] = None,
    session_manager = None,
    error_tracker = None,
    modified_after: Optional[datetime] = None
) -> Optional[str]:
    """Synchronous version of fetch_and_cache"""
    try:
        # Get existing event loop or create new one
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # No event loop exists, create one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        # Run the coroutine
        if loop.is_running():
            # Create a future if loop already running
            future = asyncio.run_coroutine_threadsafe(
                fetch_and_cache(url, content_type, lastmod, force_refresh,
                               timeout, retries, max_size, proxy, None,
                               session_manager, error_tracker, modified_after),
                loop
            )
            return future.result(timeout or REQUEST_TIMEOUT + 5)
        else:
            # Use normal run if loop not running
            return loop.run_until_complete(
                fetch_and_cache(url, content_type, lastmod, force_refresh,
                               timeout, retries, max_size, proxy, None,
                               session_manager, error_tracker, modified_after)
            )
    except Exception as e:
        LOGGER.error(f"Error in fetch_sync for {url}: {e}", exc_info=True)
        return None


async def bulk_fetch(
    urls: List[str],
    content_type: str,
    concurrency: int = 5,
    **kwargs
) -> Dict[str, Optional[str]]:
    """
    Fetch multiple URLs concurrently with rate limiting.
    
    Args:
        urls: List of URLs to fetch
        content_type: Content type for all URLs
        concurrency: Maximum number of concurrent requests
        **kwargs: Additional arguments to pass to fetch_and_cache
        
    Returns:
        Dictionary mapping URLs to fetched content
    """
    if not urls:
        return {}
        
    # Adjust concurrency based on environment
    if IN_DOCKER:
        # More conservative in Docker
        concurrency = min(concurrency, 3)
    
    # Create a shared session for efficiency
    session = await create_session(kwargs.get('proxy'))
    
    try:
        # Use semaphore to limit concurrency
        semaphore = asyncio.Semaphore(concurrency)
        
        # Create fetch tasks with rate limiting
        async def fetch_with_semaphore(url):
            async with semaphore:
                # Add delay between requests to prevent rate limiting
                await asyncio.sleep(random.uniform(0.5, 1.5))
                try:
                    content = await fetch_and_cache(url, content_type, session=session, **kwargs)
                    return url, content
                except Exception as e:
                    LOGGER.error(f"Error fetching {url} in bulk_fetch: {e}")
                    return url, None
        
        # Create tasks
        tasks = [fetch_with_semaphore(url) for url in urls]
        
        # Wait for all tasks to complete
        results = {}
        for task in asyncio.as_completed(tasks):
            try:
                url, content = await task
                results[url] = content
            except Exception as e:
                LOGGER.error(f"Error processing bulk fetch task: {e}")
            
        return results
    except Exception as e:
        LOGGER.error(f"Error in bulk_fetch: {e}", exc_info=True)
        return {}
    finally:
        # Clean up session
        if session:
            try:
                await session.close()
            except Exception as e:
                LOGGER.warning(f"Error closing session in bulk_fetch: {e}")


@contextlib.contextmanager
def safe_file_read(file_path: str, chunk_size: int = 8192):
    """
    Read a file in a memory-efficient way using a context manager.
    
    Args:
        file_path: Path to the file
        chunk_size: Size of chunks to read
        
    Yields:
        File contents as string
    """
    try:
        chunks = []
        with open(file_path, 'r', encoding='utf-8') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)
        yield ''.join(chunks)
    except UnicodeDecodeError:
        # If UTF-8 fails, try with a more forgiving encoding
        chunks = []
        with open(file_path, 'r', encoding='latin-1') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)
        yield ''.join(chunks)
    except Exception as e:
        LOGGER.error(f"Error reading file {file_path}: {e}")
        yield None


def clear_cache(content_type: Optional[str] = None, older_than_days: Optional[int] = None) -> int:
    """
    Clear cache files.
    
    Args:
        content_type: Specific content type to clear, or None for all
        older_than_days: Only clear files older than this many days
        
    Returns:
        Number of files deleted
    """
    import shutil
    from datetime import timedelta
    
    count = 0
    
    try:
        # Determine which directories to clear
        if content_type:
            cache_dir = CACHE_BASE / content_type
            if not cache_dir.exists():
                LOGGER.warning(f"Cache directory for {content_type} doesn't exist")
                return 0
            directories = [cache_dir]
        else:
            if not CACHE_BASE.exists():
                LOGGER.warning("Main cache directory doesn't exist")
                return 0
            directories = [CACHE_BASE / d for d in os.listdir(CACHE_BASE) if (CACHE_BASE / d).is_dir()]
        
        # Calculate cutoff date
        cutoff_date = None
        if older_than_days:
            cutoff_date = datetime.now() - timedelta(days=older_than_days)
        
        # Process each directory
        for directory in directories:
            if not os.path.exists(directory):
                continue
                
            if older_than_days:
                # Delete individual files older than cutoff
                for filename in os.listdir(directory):
                    file_path = directory / filename
                    if os.path.isfile(file_path):
                        try:
                            file_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
                            if file_modified < cutoff_date:
                                os.unlink(file_path)
                                count += 1
                        except (OSError, IOError) as e:
                            LOGGER.warning(f"Error removing cache file {file_path}: {e}")
            else:
                # Delete entire directory
                try:
                    file_count = len([f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))])
                    count += file_count
                    shutil.rmtree(directory)
                    os.makedirs(directory, exist_ok=True)
                except (OSError, IOError) as e:
                    LOGGER.error(f"Error clearing cache directory {directory}: {e}")
        
        LOGGER.info(f"Cleared {count} cache {'files' if older_than_days else 'directories'}")
        return count
        
    except Exception as e:
        LOGGER.error(f"Error clearing cache: {e}")
        return count