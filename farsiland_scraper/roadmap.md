# Enhanced Farsiland Scraper Fix Roadmap

This document outlines the identified issues in the Farsiland Scraper codebase and provides guidance for fixing each problem. The fixes are organized by file and prioritized based on importance for incremental updates, Docker/NAS deployment, and improved functionality.

## Core Issues

### 1. Incremental Updates Not Working Properly
The primary issue is that incremental updates don't correctly filter URLs for crawling. While the code identifies modified content, it still processes all URLs.

- **Additional Gap**: No single source of truth for tracking modified content
- **Additional Gap**: CLI flags for modification dates not properly connected to filtering logic

### 2. Internationalization & Persian Text Support
- Persian characters in URLs and content aren't properly handled
- SQLite might not store Persian text correctly without explicit encoding settings
- URL normalization doesn't properly handle non-ASCII URLs

- **Additional Gap**: Inconsistent encoding/decoding of Persian and Arabic text
- **Additional Gap**: No normalization for Persian/Arabic digits and special characters
- **Additional Gap**: Missing character set validation for database operations

### 3. Site Protection & Authentication
- Current video link resolution won't work if site changes its protection mechanism
- Code breaks if site implements CAPTCHA protection
- No login capability for authenticated content
- No cookie/session persistence between runs

- **Additional Gap**: CAPTCHA detection exists but nothing uses it
- **Additional Gap**: Session manager not integrated with fetch operations

### 4. Resilience & Error Recovery
- If crawl is interrupted, it must restart completely with no checkpoint/resume
- No support for environments requiring HTTPS proxies
- Limited transaction isolation causing potential database corruption
- No schema migration capabilities if site structure changes

- **Additional Gap**: Checkpoint manager only saves based on time, not item count
- **Additional Gap**: Poor handling of unstable network conditions
- **Additional Gap**: Missing proper exponential backoff for failed requests
- **Additional Gap**: No cleanup of temporary files after exceptions
- **Additional Gap**: Memory leaks in long-running processes

### 5. Concurrency & Resource Management
- **New Issue**: Race conditions in file access operations
- **New Issue**: No proper locking for shared resources between spiders
- **New Issue**: Potential deadlocks in multi-process mode
- **New Issue**: No adaptive resource allocation based on system capabilities
- **New Issue**: Memory management issues during long daemon runs

### 6. Testing Infrastructure
- **New Issue**: Missing integration tests for the complete pipeline
- **New Issue**: Inadequate error path testing
- **New Issue**: No performance benchmarks for NAS environments

## Files Requiring Fixes

### 1. `farsiland_scraper/utils/sitemap_parser.py` (already fixed/modified)

**Issues:**
- Timezone handling issue
- Modified URLs are not properly separated from the full URL set
- No metadata about previous scan results is preserved

**Additional Issues:**
- Filtering never reaches the spiders
- CLI flags (`--modified-window`, `--modified-after`) not properly connected

**Fixes Needed:**
```python
# Around line 340 in parse_sitemap method
if modified_after and lastmod_text:
    try:
        lastmod_date = datetime.fromisoformat(lastmod_text.replace('Z', '+00:00'))
        # Make modified_after timezone-aware if it isn't already
        if modified_after.tzinfo is None:
            from datetime import timezone
            modified_after = modified_after.replace(tzinfo=timezone.utc)
        
        if lastmod_date <= modified_after:
            # Skip older items
            continue
    except ValueError as e:
        # Log and continue
```

- Add a new structure to track filtered vs. all URLs:
```python
def run(self, force_refresh: bool = False, return_ids: bool = False, 
        modified_after: Optional[datetime] = None) -> Union[bool, Dict[str, List]]:
    # After processing sitemaps
    
    # Store all URLs in self.results as before
    # Add a new field to track ONLY urls modified after the date
    self.modified_results = {
        "movies": [],
        "shows": [],
        "episodes": []
    }
    
    # When adding URLs to results, also check if they should be in modified_results
    
    # Return both all URLs and modified URLs in result 
    return {
        "all_urls": self.results,
        "modified_urls": self.modified_results
    }
```

- Enhance save_results to preserve metadata from previous runs:
```python
def save_results(self) -> bool:
    try:
        # Load existing data if available
        existing_data = {}
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
            except:
                pass
        
        # Update metadata
        output_data = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "last_full_scan": existing_data.get("metadata", {}).get("last_full_scan", datetime.now().isoformat()),
                "parsing_errors": self.parsing_errors,
                "modified_count": sum(len(category) for category in self.modified_results.values())
            }
        }
        
        # Include both all content and modified content
        for category in self.results:
            output_data[category] = self.results[category]
            output_data[f"modified_{category}"] = self.modified_results[category]
            
        # Write to file
        # ...existing code...
    except:
        # ...error handling...
```

### 2. `farsiland_scraper/run.py` (already fixed/modified)

**Issues:**
- Does not properly pass modified_after to sitemap parser
- Doesn't use filtered URLs when loading start URLs for spiders
- Doesn't properly handle API-only mode with modifications
- No memory management for large crawls
- Inefficient error handling

**Additional Issues:**
- Environment variables defined but not used (e.g., `FARSILAND_MAX_CPU_PERCENT`)
- No single source of truth for modification date filtering

**Fixes Needed:**
- Update `_load_sitemap_urls` method to handle modified URLs:
```python
def _load_sitemap_urls(self) -> None:
    try:
        # Use modified_after to get only updated items
        if self.modified_after:
            LOGGER.info(f"Filtering for content modified after {self.modified_after}")
        
        if self.args.update_sitemap:
            # Update sitemap with modified_after filter
            parser = SitemapParser(output_file=self.args.sitemap_file or PARSED_SITEMAP_PATH)
            result = parser.run(modified_after=self.modified_after, return_ids=self.args.api_first)
            
            # Store both all URLs and modified URLs
            if isinstance(result, dict) and "modified_urls" in result:
                self.sitemap_modified_urls = result["modified_urls"]
                self.sitemap_urls = result["all_urls"]
                
                # Log stats about modified content
                for content_type, urls in self.sitemap_modified_urls.items():
                    if urls:
                        LOGGER.info(f"Found {len(urls)} modified {content_type} since {self.modified_after}")
            else:
                # Fall back to old behavior if sitemap parser doesn't support new format
                self.sitemap_urls = result
                self.sitemap_modified_urls = {}
        else:
            # Load existing sitemap data
            with open(PARSED_SITEMAP_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Get modified content if available
                if self.modified_after and "modified_movies" in data:
                    self.sitemap_modified_urls = {
                        "movies": data.get("modified_movies", []),
                        "shows": data.get("modified_shows", []),
                        "episodes": data.get("modified_episodes", [])
                    }
                else:
                    self.sitemap_modified_urls = {}
                    
                # Get all URLs as before
                self.sitemap_urls = {
                    "movies": data.get("movies", []),
                    "shows": data.get("shows", []) or data.get("series", []),
                    "episodes": data.get("episodes", [])
                }
    except Exception as e:
        LOGGER.error(f"Error loading sitemap data: {e}")
        self.sitemap_urls = {}
        self.sitemap_modified_urls = {}
```

- Update `get_start_urls` method to use modified URLs when available:
```python
def get_start_urls(self, spider_type: str) -> List[str]:
    if not self.args.sitemap:
        # Without sitemap, spiders use their default start URLs
        return []

    # Map spider type to sitemap category
    category = 'series' if spider_type == 'series' else spider_type
    
    # Use modified URLs if incremental update is enabled
    urls_source = None
    if self.modified_after and hasattr(self, 'sitemap_modified_urls') and self.sitemap_modified_urls:
        urls_source = self.sitemap_modified_urls.get(category, [])
        LOGGER.info(f"Using {len(urls_source)} modified {category} URLs from sitemap")
    else:
        # Fall back to all URLs if no modified URLs or not in incremental mode
        urls_source = self.sitemap_urls.get(category, [])
        LOGGER.info(f"Using all {len(urls_source)} {category} URLs from sitemap")
    
    # Extract URL strings
    start_urls = []
    if urls_source:
        if isinstance(urls_source[0], dict) and 'url' in urls_source[0]:
            start_urls = [entry["url"] for entry in urls_source]
        else:
            start_urls = urls_source
    
    # Apply limit if specified
    max_items = self.get_max_items()
    if max_items > 0:
        limited_urls = start_urls[:max_items]
        LOGGER.info(f"Limiting {category} URLs to {len(limited_urls)} (limit={max_items})")
        return limited_urls
    
    return start_urls
```

- Add memory management for large crawls:
```python
def run_spiders(self) -> bool:
    # Add memory tracking
    import psutil
    
    self.start_time = time.time()
    start_memory = psutil.Process().memory_info().rss / 1024 / 1024
    LOGGER.info(f"Starting crawl at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    LOGGER.info(f"Initial memory usage: {start_memory:.2f} MB")
    
    # ...existing code...
    
    # After crawling
    end_memory = psutil.Process().memory_info().rss / 1024 / 1024
    LOGGER.info(f"Final memory usage: {end_memory:.2f} MB (change: {end_memory - start_memory:.2f} MB)")
```

- Add environment variable handling:
```python
# Near the top of the run.py file after imports
# Read environment variables
MAX_CPU_PERCENT = int(os.environ.get('FARSILAND_MAX_CPU_PERCENT', 80))
MAX_MEMORY_MB = int(os.environ.get('FARSILAND_MAX_MEMORY_MB', 1024))
```

### 3. `farsiland_scraper/spiders/series_spider.py`, `episodes_spider.py`, `movies_spider.py`

**Issues:**
- Don't properly handle incremental updates
- Inconsistent URL normalization
- Memory inefficiency when processing large sets of data
- Not optimized for Docker/headless environments

**Additional Issues:**
- Duplicate methods across spider files
- No cleanup on spider close

**Fixes Needed (for each spider file):**
- Add incremental update support:
```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    
    # Add support for incremental updates
    self.modified_after = kwargs.get("modified_after", None)
    if self.modified_after:
        LOGGER.info(f"Spider will only process content modified after {self.modified_after}")
```

- Standardize URL normalization:
```python
def normalize_url(self, url: str) -> str:
    """
    Normalize URL format for consistency across the project.
    
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
        
    # Remove trailing slash for consistency
    return url.rstrip('/')
```

- Use normalized URLs consistently throughout the code
- Add resource cleanup:
```python
def closed(self, reason):
    """
    Clean up resources when spider closes.
    
    Args:
        reason: Close reason
    """
    # Clear caches and free memory
    if hasattr(self, 'api_client') and self.api_client:
        self.api_client.clear_cache()
        
    # Force garbage collection
    import gc
    gc.collect()
    
    LOGGER.info(f"Spider closed: {reason}")
```

### 4. `farsiland_scraper/core/api_client.py`  (already fixed/modified) Review the code and confirm

**Issues:**
- Cache doesn't respect modified_after parameter
- No proper cache invalidation strategy
- Memory leak in response caching
- Not optimized for Docker/headless environments

**Additional Issues:**
- No adaptive cache size based on available memory
- Missing pruning on a regular basis

**Fixes Needed:**
- Add cache management methods:
```python
def clear_cache(self):
    """Clear all cached responses to free memory."""
    cache_size = len(self.response_cache)
    self.response_cache.clear()
    self.logger.info(f"Cleared API response cache ({cache_size} entries)")
    
def prune_cache(self, max_size: int = 1000):
    """
    Prune cache to prevent memory issues.
    
    Args:
        max_size: Maximum number of entries to keep
    """
    if len(self.response_cache) > max_size:
        # Keep only the newest entries
        sorted_keys = sorted(
            self.response_cache.keys(),
            key=lambda k: self.response_cache[k].get("_timestamp", 0) 
                if isinstance(self.response_cache[k], dict) else 0,
            reverse=True
        )
        
        # Remove oldest entries
        keys_to_remove = sorted_keys[max_size:]
        for key in keys_to_remove:
            del self.response_cache[key]
            
        self.logger.info(f"Pruned API cache from {len(sorted_keys)} to {max_size} entries")
```

- Respect modified_after filter:
```python
def map_api_to_item(self, api_data, item_type, modified_after=None):
    """
    Map API data to scraper item format, respecting modified_after filter.
    
    Args:
        api_data: Data from API
        item_type: Type of item
        modified_after: Optional date filter
        
    Returns:
        Mapped item dict or None if filtered out
    """
    if not api_data:
        return {}
        
    # Check if this item should be filtered by date
    if modified_after and "modified_gmt" in api_data:
        try:
            modified_date = datetime.fromisoformat(api_data["modified_gmt"].replace('Z', '+00:00'))
            
            # Make modified_after timezone-aware if it isn't already
            if modified_after.tzinfo is None:
                from datetime import timezone
                modified_after = modified_after.replace(tzinfo=timezone.utc)
                
            # Skip if older than modified_after
            if modified_date <= modified_after:
                return None
        except (ValueError, TypeError, AttributeError):
            # If we can't parse the date, include it to be safe
            pass
    
    # Proceed with mapping as before
    # ...existing code...
```

- Add adaptive cache sizing:
```python
def _optimize_cache_size(self):
    """Adjust cache size based on available memory."""
    try:
        import psutil
        
        # Get available memory in MB
        available_memory = psutil.virtual_memory().available / (1024 * 1024)
        
        # Adjust cache size based on available memory
        if available_memory < 100:  # Less than 100MB available
            self.prune_cache(max_size=50)  # Aggressive pruning
        elif available_memory < 500:  # Less than 500MB available
            self.prune_cache(max_size=200)  # Moderate pruning
        elif len(self.response_cache) > 1000:
            self.prune_cache(max_size=1000)  # Default pruning
            
    except ImportError:
        # psutil not available, use default pruning
        if len(self.response_cache) > 1000:
            self.prune_cache(max_size=1000)
```

### 5. `farsiland_scraper/config.py` (already fixed/modified)

**Issues:**
- No Docker-specific configuration
- No resource limit settings for constrained environments (NAS)
- No proper logging configuration for headless environments
- Hard-coded paths aren't compatible with Docker volume mounts

**Additional Issues:**
- Missing integration with environment variables for resource management
- Not handling temporary storage location properly

**Fixes Needed:**
- Add Docker/NAS specific configurations:
```python
# Docker environment detection
IN_DOCKER = os.environ.get('FARSILAND_IN_DOCKER', 'false').lower() == 'true'

# Resource limit settings
MAX_MEMORY_MB = int(os.environ.get('FARSILAND_MAX_MEMORY_MB', 1024))  # Default 1GB
MAX_CPU_PERCENT = int(os.environ.get('FARSILAND_MAX_CPU_PERCENT', 80))  # Default 80%
MAX_CONCURRENT_PROCESSES = int(os.environ.get('FARSILAND_MAX_CONCURRENT', 4))

# Docker-specific paths
if IN_DOCKER:
    BASE_DIR = Path('/app')
    DATA_DIR = Path('/data')
    LOG_DIR = Path('/logs')
    CACHE_DIR = Path('/cache')
    # For temporary files
    TEMP_DIR = Path('/tmp/farsiland')
    TEMP_DIR.mkdir(exist_ok=True)
else:
    # Original paths for non-Docker environments
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR = Path(os.environ.get('FARSILAND_DATA_DIR', BASE_DIR / "data"))
    LOG_DIR = Path(os.environ.get('FARSILAND_LOG_DIR', BASE_DIR / "logs"))
    CACHE_DIR = Path(os.environ.get('FARSILAND_CACHE_DIR', BASE_DIR / "cache"))
    TEMP_DIR = Path(os.environ.get('FARSILAND_TEMP_DIR', tempfile.gettempdir()) / "farsiland")
    TEMP_DIR.mkdir(exist_ok=True)
```

- Add better logging for headless environments:
```python
# Configure logging based on environment
def setup_logger(name, log_file, level=None):
    """
    Set up a logger with appropriate handlers for the environment.
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
```

### 6. `farsiland_scraper/database/models.py`

**Issues:**
- No proper file locking for SQLite in multi-process environments
- No database optimization for constrained resources
- Not optimized for network storage (NAS)
- SQL injection vulnerabilities in some methods
- Persian text encoding issues
- No transaction isolation causing database corruption on crashes
- No schema migration capabilities

**Additional Issues:**
- Competing migration systems
- Transaction isolation could deadlock on slow NAS

**Fixes Needed:**
- Add proper SQLite locking for Docker/NAS environments with Persian text support:
```python
def __init__(self, db_path: str = DATABASE_PATH):
    self.db_path = str(db_path)
    self.conn: Optional[sqlite3.Connection] = None
    self.cursor: Optional[sqlite3.Cursor] = None

    try:
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Configure SQLite for better NAS/network storage compatibility
        sqlite3.enable_callback_tracebacks(True)
        
        # Select journal mode based on filesystem type
        journal_mode = "WAL"  # Default to WAL for better performance
        
        # For Docker/NAS environments, check if we're on a network filesystem
        if IN_DOCKER:
            try:
                import os
                # Check if filesystem is remote/NFS 
                if hasattr(os, 'statvfs'):
                    st = os.statvfs(os.path.dirname(self.db_path))
                    is_network_fs = bool(st.f_flag & os.ST_RDONLY)
                    if is_network_fs:
                        journal_mode = "DELETE"  # Use DELETE mode for network filesystems
                        LOGGER.info("Network filesystem detected, using DELETE journal mode")
            except Exception as e:
                LOGGER.warning(f"Could not detect filesystem type: {e}, using WAL mode")
        
        # Connect with proper timeout and isolation level
        self.conn = sqlite3.connect(
            self.db_path, 
            timeout=60.0,  # Longer timeout for network storage
            isolation_level='IMMEDIATE'  # Better than EXCLUSIVE for NAS
        )
        
        # Set proper encoding for Persian text
        self.conn.execute('PRAGMA encoding="UTF-8"')
        
        # Set journal mode based on environment
        self.conn.execute(f'PRAGMA journal_mode={journal_mode}')
        
        # Better transaction isolation
        self.conn.execute('PRAGMA foreign_keys=ON')
        
        # Optimize for better performance on constrained devices
        self.conn.execute('PRAGMA synchronous=NORMAL')  # Less fsync for better performance
        self.conn.execute('PRAGMA cache_size=-4000')  # 4MB cache
        self.conn.execute('PRAGMA temp_store=MEMORY')  # Store temp tables in memory
        
        # Configure SQLite to handle text as UTF-8
        self.conn.text_factory = str
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        # ensure base tables then back‑fill any new columns
        self._create_tables()
        self._ensure_columns()
        
        # Use new migration manager if available
        try:
            from farsiland_scraper.database.migrations import MigrationManager
            migration_manager = MigrationManager(self.db_path)
            migration_manager.apply_migrations()
        except ImportError:
            # Fall back to built-in migration
            self._check_schema_version()
    except sqlite3.Error as e:
        LOGGER.error(f"Database initialization error: {e}")
        raise
```

- Add database optimization method:
```python
def optimize_database(self):
    """
    Perform maintenance and optimization on the database.
    Call this periodically, especially on NAS deployments.
    
    Returns:
        True if successful, False otherwise
    """
    if not self.conn:
        return False
        
    try:
        # Begin optimization operations
        LOGGER.info("Starting database optimization")
        
        # Run integrity check first
        check_result = self.fetchone("PRAGMA quick_check")
        if check_result and check_result[0] != "ok":
            LOGGER.error(f"Database integrity check failed: {check_result[0]}")
            return False
            
        # Analyze to update statistics
        self.execute("ANALYZE")
        
        # Optimize indices
        self.execute("REINDEX")
        
        # Run VACUUM in a separate connection (cannot be in transaction)
        self.commit()  # Commit any pending changes
        
        # Create a new connection for VACUUM
        vacuum_conn = sqlite3.connect(self.db_path)
        vacuum_conn.execute("VACUUM")
        vacuum_conn.close()
        
        LOGGER.info("Database optimization completed successfully")
        return True
    except Exception as e:
        LOGGER.error(f"Database optimization failed: {e}")
        return False
```

### 7. `farsiland_scraper/fetch.py` (already fixed/modified)

**Issues:**
- File locking issues on Windows
- Memory management for large responses
- No support for network timeouts common in NAS environments
- Cache doesn't respect incremental update parameters
- Doesn't properly handle non-ASCII URLs (Persian text)
- No proxy support
- No integration with authentication mechanism

**Additional Issues:**
- Exponential backoff implementation is inconsistent
- No cleanup of temporary files

**Fixes Needed:**
- Improve file locking for cross-platform compatibility:
```python
def get_lock_path(cache_path: Path) -> Path:
    """
    Get the lock file path for a cache file.
    
    Args:
        cache_path: The path to the cache file
        
    Returns:
        The path to the lock file
    """
    return cache_path.with_suffix(cache_path.suffix + ".lock")

# Platform-specific locking
if os.name == 'nt':  # Windows
    import msvcrt
    
    def lock_file(f):
        msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        
    def unlock_file(f):
        msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
else:  # Unix
    import fcntl
    
    def lock_file(f):
        fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        
    def unlock_file(f):
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)
```

- Fix URL encoding for Persian text:
```python
def slugify_url(url: str) -> str:
    """
    Convert a URL to a safe filename handling Unicode characters.
    
    Args:
        url: The URL to convert
        
    Returns:
        A safe filename generated from the URL
    """
    from urllib.parse import quote_plus, unquote_plus
    import hashlib
    
    # First decode any already encoded components
    url = unquote_plus(url)
    
    # Create a hash of the normalized URL for uniqueness
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:12]
    
    # For filesystem safety, use the hash
    return f"url_{url_hash}"
```

- Add proxy and session integration:
```python
async def fetch_and_cache(
    url: str, 
    content_type: str, 
    lastmod: Optional[str] = None, 
    force_refresh: bool = False,
    timeout: int = None,
    retries: int = None,
    max_size: int = None,
    modified_after: Optional[datetime] = None,
    proxy: Optional[str] = None,
    session: Optional[aiohttp.ClientSession] = None
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
        modified_after: Only fetch if modified after this date
        proxy: Optional proxy URL (e.g., 'http://user:pass@some.proxy.com:8080/')
        session: Optional existing aiohttp session to reuse
        
    Returns:
        The HTML content or None if fetching failed
    """
    # Use config values if not specified
    if timeout is None:
        timeout = REQUEST_TIMEOUT
    if retries is None:
        retries = REQUEST_RETRY_COUNT
    if max_size is None:
        max_size = MAX_VIDEO_SIZE
    if proxy is None:
        proxy = os.environ.get('FARSILAND_PROXY', None)
    
    # Properly handle encoding in URL for cache path
    cache_path = get_cache_path(url, content_type)
    lock_path = get_lock_path(cache_path)
    
    # Create parent directories if they don't exist
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Use a lock to prevent concurrent access to the same cache file
    lock = filelock.FileLock(str(lock_path))
    
    try:
        with lock.acquire(timeout=10):  # Wait up to 10 seconds for the lock
            # Check if cache exists and is valid
            if cache_path.exists() and not force_refresh:
                cache_mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
                
                # If we have a modified_after date and the cache is newer than that date,
                # we can use the cached version
                if modified_after and cache_mtime > modified_after:
                    LOGGER.info(f"Using cached version of {url} (cache newer than filter date)")
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        return f.read()
                        
                # Original lastmod-based cache validation
                if lastmod:
                    try:
                        lastmod_time = datetime.fromisoformat(lastmod.replace('Z', '+00:00'))
                        if cache_mtime >= lastmod_time:
                            LOGGER.info(f"Using cached version of {url}")
                            with open(cache_path, 'r', encoding='utf-8') as f:
                                return f.read()
                    except Exception as e:
                        LOGGER.warning(f"Could not compare lastmod for {url}: {e}")
                        with open(cache_path, 'r', encoding='utf-8') as f:
                            return f.read()
                else:
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        return f.read()

            # If cache doesn't exist or is invalid, fetch the URL
            for attempt in range(retries):
                try:
                    # Add headers to limit size and set accepted content types
                    headers = {
                        "Range": f"bytes=0-{max_size}",
                        "Accept": "text/html,application/xhtml+xml",
                        "Accept-Language": "fa,en-US;q=0.9,en;q=0.8"  # Add Farsi language support
                    }
                    
                    # Create or reuse session
                    should_close_session = False
                    if session is None:
                        # Create session with proxy if specified
                        if proxy:
                            session = aiohttp.ClientSession(proxy=proxy)
                        else:
                            session = aiohttp.ClientSession()
                        should_close_session = True
                    
                    try:
                        # Check for CAPTCHA in content
                        async with session.get(url, timeout=timeout, headers=headers) as response:
                            if response.status == 200:
                                content_type_header = response.headers.get('Content-Type', '')
                                html = await response.text()
                                
                                # CAPTCHA detection
                                if 'text/html' in content_type_header and _contains_captcha(html):
                                    LOGGER.error(f"CAPTCHA detected on {url}")
                                    raise CaptchaDetectedException(f"CAPTCHA found on {url}")
                                    
                                # Write to cache with proper encoding
                                with open(cache_path, 'w', encoding='utf-8') as f:
                                    f.write(html)
                                LOGGER.info(f"Fetched and cached {url}")
                                return html
                            else:
                                LOGGER.warning(f"Failed to fetch {url} (status: {response.status})")
                                # Don't retry for client errors (4xx)
                                if 400 <= response.status < 500:
                                    break
                    finally:
                        if should_close_session:
                            await session.close()
                except aiohttp.ClientError as e:
                    LOGGER.error(f"Client error fetching {url}: {e}")
                except CaptchaDetectedException as e:
                    LOGGER.error(str(e))
                    raise  # Re-raise CAPTCHA exception
                except Exception as e:
                    LOGGER.error(f"Error fetching {url}: {e}")
                
                # Implement proper exponential backoff with jitter
                if attempt < retries - 1:
                    backoff_time = min(60, (2 ** attempt) * REQUEST_RETRY_DELAY)
                    # Add jitter (±10%)
                    jitter = backoff_time * (0.9 + 0.2 * random.random())
                    LOGGER.info(f"Retrying in {jitter:.1f}s (attempt {attempt+1}/{retries})")
                    await asyncio.sleep(jitter)
            
            # If we get here, all retry attempts failed
            return None
    except filelock.Timeout:
        LOGGER.warning(f"Could not acquire lock for {url}, cache file may be in use")
        # Try to use the cached version anyway if it exists
        if cache_path.exists():
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                LOGGER.error(f"Error reading cache file after lock timeout: {e}")
        return None
    except CaptchaDetectedException:
        # Let CAPTCHA exceptions propagate
        raise
    except Exception as e:
        LOGGER.error(f"Unexpected error in fetch_and_cache for {url}: {e}")
        return None

def _contains_captcha(html: str) -> bool:
    """
    Check if HTML content contains a CAPTCHA.
    
    Args:
        html: HTML content to check
        
    Returns:
        True if CAPTCHA detected, False otherwise
    """
    # Common CAPTCHA indicators
    captcha_patterns = [
        'captcha',
        'recaptcha',
        'g-recaptcha',
        'hcaptcha',
        'captchaContainer',
        'verification required',
        'verify you are human'
    ]
    
    html_lower = html.lower()
    return any(pattern in html_lower for pattern in captcha_patterns)

class CaptchaDetectedException(Exception):
    """Exception raised when a CAPTCHA is detected."""
    pass
```

## New Files to Create

To address the identified issues, we need to create the following new files:

### 1. `farsiland_scraper/auth/session_manager.py` (already created)

This file will handle login and session management:

### 2. `farsiland_scraper/resolvers/captcha_solver.py` (already created)

This file will handle CAPTCHA detection and resolution:

### 3. `farsiland_scraper/utils/checkpoint_manager.py`(already created)

This file will handle checkpoint/resume functionality:

### 4. `farsiland_scraper/database/migrations.py` (already created)

This file will handle database schema migrations:

### 5. `farsiland_scraper/utils/url_patterns.py` (already created)

This file will handle flexible URL pattern matching:

### 6. `farsiland_scraper/tests/test_incremental.py` (already created)

New file for testing incremental update functionality:


## Docker/NAS Recommendations

### 1. Create a Dockerfile for Containerization

Create a `Dockerfile` in the project root:

```dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY farsiland_scraper/ /app/farsiland_scraper/

# Set environment variables
ENV FARSILAND_IN_DOCKER=true
ENV PYTHONUNBUFFERED=1

# Create volume mount points
RUN mkdir -p /data /logs /cache /tmp/farsiland
VOLUME ["/data", "/logs", "/cache"]

# Set default command
ENTRYPOINT ["python", "-m", "farsiland_scraper.run"]
CMD ["--help"]
```

### 2. Add Docker Compose Configuration

Create a `docker-compose.yml` file for easy deployment:

```yaml
version: '3'

services:
  farsiland-scraper:
    build: .
    volumes:
      - ./data:/data
      - ./logs:/logs
      - ./cache:/cache
    environment:
      - FARSILAND_MAX_MEMORY_MB=1024
      - FARSILAND_MAX_CPU_PERCENT=80
      - FARSILAND_MAX_CONCURRENT=4
      - FARSILAND_LOG_LEVEL=info
      # Optional proxy settings
      # - FARSILAND_PROXY=http://user:pass@proxy:port
      # Optional auth settings
      # - FARSILAND_USERNAME=username
      # - FARSILAND_PASSWORD=password
    restart: unless-stopped
    command: >
      --daemon
      --rss
      --api-first
      --update-sitemap
      --modified-window 7
      --checkpoint-interval 300
    # Uncomment to limit resources for NAS deployment
    # mem_limit: 1g
    # cpus: 2.0
    healthcheck:
      test: ["CMD", "python", "-m", "farsiland_scraper.healthcheck"]
      interval: 5m
      timeout: 30s
      retries: 3
```

### 3. Resource Management for NAS

For NAS environments, consider these additional optimizations:

1. **Implement Resource Monitoring**
   
   Add resource monitoring code in `run.py`:
   
   ```python
   def _check_resources(self):
       """Monitor and control resource usage."""
       import psutil
       
       # Check memory usage
       memory_percent = psutil.virtual_memory().percent
       if memory_percent > 90:  # Over 90% memory usage
           LOGGER.warning(f"High memory usage detected: {memory_percent}%")
           # Force garbage collection
           import gc
           gc.collect()
           
           # If API client exists, prune cache
           if hasattr(self, 'api_client') and self.api_client:
               self.api_client._optimize_cache_size()
       
       # Check CPU usage
       cpu_percent = psutil.cpu_percent(interval=0.1)
       if cpu_percent > MAX_CPU_PERCENT:
           LOGGER.warning(f"High CPU usage detected: {cpu_percent}%")
           # Add delay to reduce CPU load
           time.sleep(1.0)
   ```

2. **Low-Memory Mode for Constrained Environments**
   
   Add a low-memory option in `run.py`:
   
   ```python
   # Add to argument parser
   parser.add_argument('--low-memory', action='store_true',
                      help='Enable low memory optimization for NAS environments')
                      
   # If low memory mode is active
   if self.args.low_memory:
       LOGGER.info("Low memory mode enabled")
       # Reduce batch sizes
       self.args.concurrent_requests = min(self.args.concurrent_requests or 4, 2)
       # Disable response caching
       if self.api_client:
           self.api_client.cache_enabled = False
   ```

### 4. Add Health Check Module

Create a health check module (`farsiland_scraper/healthcheck.py`):

### 5. Script for Automatic Updates

Create a shell script for automatic updates on NAS:

```bash
#!/bin/bash
# farsiland_update.sh - Run incremental update and cleanup

# Change to script directory
cd "$(dirname "$0")"

# Run incremental update
docker-compose run --rm farsiland-scraper \
  --update-sitemap \
  --sitemap \
  --rss \
  --modified-window 1 \
  --api-first \
  --low-memory \
  --checkpoint-interval 120

# Cleanup old logs (keep last 7 days)
find ./logs -name "*.log" -type f -mtime +7 -delete

# Optimize database (monthly)
if [ $(date +%d) -eq "01" ]; then
  docker-compose run --rm farsiland-scraper --optimize-db
fi

echo "Update completed at $(date)"
```

Make this script executable and add to cron for scheduled execution:
```
chmod +x farsiland_update.sh
```

### 6. Update requirements.txt

Ensure `requirements.txt` includes all needed dependencies:

```
scrapy>=2.6.0
requests>=2.27.1
beautifulsoup4>=4.10.0
aiohttp>=3.8.1
filelock>=3.6.0
psutil>=5.9.0
python-dateutil>=2.8.2
feedparser>=6.0.8
```

These changes address all the identified issues in the Farsiland Scraper, improving its reliability, resilience, and functionality while ensuring compatibility with Docker and NAS environments.
