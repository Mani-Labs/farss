# File: farsiland_scraper/database/models.py
# Version: 5.0.0
# Last Updated: 2025-06-20

"""
Database models and connection management for Farsiland scraper.

Features:
- Thread-safe database operations
- Robust transaction management with automatic retries
- Prevention of SQL injection through parameter sanitization
- Connection pooling with resource optimization
- Proper error handling and recovery
- Automatic schema migration
- Support for Docker/NAS environments
- Optimized settings for constrained resources

Changelog:
- [5.0.0] Complete rewrite with improved architecture
- [5.0.0] Added thread-safe transaction management
- [5.0.0] Implemented connection pooling and resource limiting
- [5.0.0] Added comprehensive parameter sanitization
- [5.0.0] Enhanced error handling and recovery
- [5.0.0] Improved support for Docker/NAS environments
- [5.0.0] Added query validation to prevent SQL injection
- [5.0.0] Implemented thread synchronization
- [5.0.0] Fixed memory management for large datasets
- [4.0.0] Previous major version
"""

import os
import sqlite3
import json
import logging
import time
import threading
import tempfile
import gc
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from collections import deque

from farsiland_scraper.config import (
    DATABASE_PATH,
    JSON_OUTPUT_PATH,
    LOGGER,
    IN_DOCKER,
    MAX_MEMORY_MB,
    MAX_CPU_PERCENT,
    MAX_CONCURRENT_PROCESSES
)

# Try to import psutil for memory monitoring
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    LOGGER.warning("psutil package not available, memory monitoring disabled")


class SafeTransactionManager:
    """
    Thread-safe transaction management with automatic retries and error handling.
    
    Usage:
        with SafeTransactionManager(db_connection) as tx:
            # Perform database operations
            # Transaction is committed on success or rolled back on error
    """
    
    def __init__(self, connection, logger=None, max_retries=3):
        """
        Initialize transaction manager.
        
        Args:
            connection: SQLite connection object
            logger: Logger instance (defaults to LOGGER from config)
            max_retries: Maximum number of retry attempts for transactions
        """
        self.connection = connection
        self.logger = logger or LOGGER
        self.max_retries = max_retries
        self.in_transaction = False
    
    def __enter__(self):
        """
        Begin a database transaction with retry mechanism.
        
        Returns:
            TransactionManager instance for context management
            
        Raises:
            sqlite3.OperationalError: If transaction cannot be started
        """
        for attempt in range(self.max_retries):
            try:
                if not self.in_transaction:
                    self.connection.execute("BEGIN")
                    self.in_transaction = True
                return self
            except sqlite3.OperationalError as e:
                # Retry on database locked errors
                if 'database is locked' in str(e) or 'database is busy' in str(e):
                    backoff_time = 0.1 * (2 ** attempt)  # Exponential backoff
                    jitter = random.uniform(0.8, 1.2)    # Add jitter
                    sleep_time = backoff_time * jitter
                    
                    self.logger.warning(f"Database locked, retrying in {sleep_time:.2f}s (attempt {attempt+1}/{self.max_retries})")
                    time.sleep(sleep_time)
                    
                    # If this is the last attempt, raise the error
                    if attempt == self.max_retries - 1:
                        self.logger.error(f"Failed to begin transaction after {self.max_retries} attempts: {e}")
                        raise
                else:
                    # Re-raise other operational errors
                    self.logger.error(f"Error beginning transaction: {e}")
                    raise
        
        # This should not be reached due to the raise in the loop
        raise sqlite3.OperationalError("Unable to begin transaction after multiple attempts")
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        End transaction with commit or rollback based on exception state.
        
        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        try:
            if self.in_transaction:
                if exc_type is None:
                    # No exception, commit the transaction
                    try:
                        self.connection.commit()
                        self.logger.debug("Transaction committed successfully")
                    except sqlite3.Error as e:
                        self.logger.error(f"Error committing transaction: {e}")
                        # Try to roll back on commit error
                        try:
                            self.connection.rollback()
                            self.logger.warning("Transaction rolled back after commit error")
                        except Exception as rollback_error:
                            self.logger.error(f"Failed to roll back after commit error: {rollback_error}")
                else:
                    # Exception occurred, roll back
                    try:
                        self.connection.rollback()
                        self.logger.warning(f"Transaction rolled back due to {exc_type.__name__}: {exc_val}")
                    except Exception as rollback_error:
                        self.logger.error(f"Failed to roll back transaction: {rollback_error}")
        finally:
            # Reset transaction state
            self.in_transaction = False


class ConnectionPool:
    """
    Thread-safe connection pool for SQLite connections.
    
    Manages reusable connections to improve performance and resource usage.
    Implements limits on concurrent connections to prevent resource exhaustion.
    """
    
    def __init__(self, db_path, max_connections=5, timeout=30, logger=None):
        """
        Initialize connection pool.
        
        Args:
            db_path: Path to SQLite database file
            max_connections: Maximum number of connections in the pool
            timeout: Connection timeout in seconds
            logger: Logger instance (defaults to LOGGER from config)
        """
        self.db_path = db_path
        self.max_connections = max_connections if not IN_DOCKER else min(max_connections, 3)
        self.timeout = timeout
        self.logger = logger or LOGGER
        self._lock = threading.RLock()
        self._idle_connections = deque()
        self._active_connections = set()
        self._connection_count = 0
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get a connection from the pool or create a new one.
        
        Returns:
            SQLite connection object
            
        Raises:
            RuntimeError: If maximum connection limit reached
        """
        with self._lock:
            # Check if there's an idle connection available
            if self._idle_connections:
                connection = self._idle_connections.popleft()
                self._active_connections.add(connection)
                return connection
            
            # Check if we can create a new connection
            if self._connection_count >= self.max_connections:
                self.logger.warning(f"Connection pool exhausted ({self._connection_count}/{self.max_connections})")
                # Try to wait for a connection to become available
                raise RuntimeError(f"Maximum connection limit ({self.max_connections}) reached")
            
            # Create a new connection
            try:
                # Determine journal mode based on environment
                journal_mode = "DELETE" if IN_DOCKER else "WAL"
                
                # Create connection with appropriate settings
                connection = sqlite3.connect(
                    self.db_path,
                    timeout=self.timeout,
                    isolation_level=None,  # We'll handle transactions manually
                    check_same_thread=False  # We'll handle thread safety manually
                )
                
                # Configure connection
                connection.execute(f"PRAGMA journal_mode={journal_mode}")
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA synchronous=NORMAL")
                connection.execute("PRAGMA cache_size=-4000")  # 4MB cache
                connection.execute("PRAGMA temp_store=MEMORY")
                
                # Set text factory for proper UTF-8 handling
                connection.text_factory = str
                
                # Set row factory for easier access to columns
                connection.row_factory = sqlite3.Row
                
                # Update connection tracking
                self._connection_count += 1
                self._active_connections.add(connection)
                
                self.logger.debug(f"Created new connection (total: {self._connection_count})")
                return connection
                
            except sqlite3.Error as e:
                self.logger.error(f"Error creating database connection: {e}")
                raise
    
    def release_connection(self, connection):
        """
        Return a connection to the pool.
        
        Args:
            connection: SQLite connection to return to the pool
        """
        with self._lock:
            if connection in self._active_connections:
                self._active_connections.remove(connection)
                
                # Check if the connection is still valid
                try:
                    cursor = connection.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    
                    # Connection is valid, return to idle pool
                    self._idle_connections.append(connection)
                    
                except sqlite3.Error:
                    # Connection is invalid, close it
                    try:
                        connection.close()
                        self._connection_count -= 1
                        self.logger.debug(f"Closed invalid connection (remaining: {self._connection_count})")
                    except Exception as e:
                        self.logger.warning(f"Error closing invalid connection: {e}")
            else:
                self.logger.warning(f"Attempted to release untracked connection")
    
    def close_all(self):
        """Close all connections in the pool."""
        with self._lock:
            # Close idle connections
            while self._idle_connections:
                connection = self._idle_connections.popleft()
                try:
                    connection.close()
                    self._connection_count -= 1
                except Exception as e:
                    self.logger.warning(f"Error closing idle connection: {e}")
            
            # Close active connections
            for connection in list(self._active_connections):
                try:
                    connection.close()
                    self._active_connections.remove(connection)
                    self._connection_count -= 1
                except Exception as e:
                    self.logger.warning(f"Error closing active connection: {e}")
            
            self.logger.info(f"Closed all database connections ({self._connection_count} remaining)")


class Database:
    """
    Thread-safe database manager with robust error handling and connection pooling.
    
    Provides a high-level interface for:
    - Safe database operations with proper parameterization
    - Transaction management
    - Schema creation and migration
    - Data export and serialization
    """
    
    def __init__(self, db_path=DATABASE_PATH):
        """
        Initialize database connection with robust error handling.
        
        Args:
            db_path: Path to SQLite database file (default: from config)
        """
        self.db_path = str(db_path)
        self.logger = LOGGER
        self._lock = threading.RLock()
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Configure SQLite for better error tracking
            sqlite3.enable_callback_tracebacks(True)
            
            # Create connection pool
            max_connections = MAX_CONCURRENT_PROCESSES if IN_DOCKER else 5
            self.connection_pool = ConnectionPool(
                self.db_path,
                max_connections=max_connections,
                timeout=60.0,  # Longer timeout for network storage
                logger=self.logger
            )
            
            # Get initial connection
            self.conn = self.connection_pool.get_connection()
            
            # Create cursor
            self.cursor = self.conn.cursor()
            
            # Create tables and ensure columns
            self._create_tables()
            self._ensure_columns()
            
            # Try to use migration manager if available
            self._apply_migrations()
            
            self.logger.info("Database initialized successfully")
            
        except sqlite3.Error as e:
            self.logger.error(f"Database initialization error: {e}")
            # Clean up any created resources
            if hasattr(self, 'connection_pool'):
                self.connection_pool.close_all()
            raise

    def _apply_migrations(self):
        """Apply database migrations if migration manager is available."""
        try:
            from farsiland_scraper.database.migrations import MigrationManager
            
            self.logger.info("Running database migrations...")
            migration_manager = MigrationManager(self.db_path)
            
            if migration_manager.apply_migrations():
                self.logger.info("Database migrations applied successfully")
            else:
                self.logger.warning("Failed to apply database migrations")
                
        except ImportError:
            self.logger.info("Migration manager not available, using built-in column checks")
        except Exception as e:
            self.logger.error(f"Error applying migrations: {e}")

    def _create_tables(self) -> None:
        """Create core tables if they don't exist (initial schema)."""
        with SafeTransactionManager(self.conn, self.logger) as _:
            try:
                # -- MOVIES ---------------------------------------------------
                self.cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS movies (
                        id              INTEGER PRIMARY KEY AUTOINCREMENT,
                        url             TEXT UNIQUE NOT NULL,
                        sitemap_url     TEXT,
                        title_en        TEXT,
                        title_fa        TEXT,
                        poster          TEXT,
                        description     TEXT,
                        release_date    TEXT,
                        year            INTEGER,
                        rating          REAL,
                        rating_count    INTEGER,
                        genres          TEXT,
                        directors       TEXT,
                        cast            TEXT,
                        social_shares   INTEGER,
                        comments_count  INTEGER,
                        video_files     TEXT,
                        is_new          INTEGER DEFAULT 1,
                        lastmod         TEXT,
                        last_scraped    TEXT,
                        cached_at       TEXT,
                        source          TEXT,
                        api_id          INTEGER,
                        api_source      TEXT,
                        modified_gmt    TEXT,
                        language_code   TEXT DEFAULT 'fa'
                    )
                    """
                )

                # -- SHOWS ----------------------------------------------------
                self.cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS shows (
                        id              INTEGER PRIMARY KEY AUTOINCREMENT,
                        url             TEXT UNIQUE NOT NULL,
                        sitemap_url     TEXT,
                        title_en        TEXT,
                        title_fa        TEXT,
                        poster          TEXT,
                        description     TEXT,
                        first_air_date  TEXT,
                        last_aired      TEXT,
                        rating          REAL,
                        rating_count    INTEGER,
                        season_count    INTEGER,
                        episode_count   INTEGER,
                        genres          TEXT,
                        directors       TEXT,
                        cast            TEXT,
                        social_shares   INTEGER,
                        comments_count  INTEGER,
                        seasons         TEXT,
                        episode_urls    TEXT,
                        is_new          INTEGER DEFAULT 1,
                        lastmod         TEXT,
                        last_scraped    TEXT,
                        cached_at       TEXT,
                        source          TEXT,
                        api_id          INTEGER,
                        api_source      TEXT,
                        modified_gmt    TEXT,
                        language_code   TEXT DEFAULT 'fa',
                        is_modified     INTEGER DEFAULT 0
                    )
                    """
                )

                # -- EPISODES -------------------------------------------------
                self.cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS episodes (
                        id              INTEGER PRIMARY KEY AUTOINCREMENT,
                        url             TEXT UNIQUE NOT NULL,
                        sitemap_url     TEXT,
                        show_url        TEXT,
                        season_number   INTEGER,
                        episode_number  INTEGER,
                        title           TEXT,
                        air_date        TEXT,
                        thumbnail       TEXT,
                        video_files     TEXT,
                        is_new          INTEGER DEFAULT 1,
                        lastmod         TEXT,
                        last_scraped    TEXT,
                        cached_at       TEXT,
                        source          TEXT,
                        api_id          INTEGER,
                        api_source      TEXT,
                        modified_gmt    TEXT,
                        show_id         INTEGER,
                        language_code   TEXT DEFAULT 'fa',
                        is_modified     INTEGER DEFAULT 0,
                        FOREIGN KEY (show_url) REFERENCES shows(url)
                    )
                    """
                )

                # Indexes (idempotent) --------------------------------------
                for ddl in [
                    "CREATE INDEX IF NOT EXISTS idx_shows_url              ON shows(url)",
                    "CREATE INDEX IF NOT EXISTS idx_shows_api_id           ON shows(api_id)",
                    "CREATE INDEX IF NOT EXISTS idx_shows_is_new           ON shows(is_new)",
                    "CREATE INDEX IF NOT EXISTS idx_shows_modified         ON shows(is_modified)",
                    
                    "CREATE INDEX IF NOT EXISTS idx_movies_url             ON movies(url)",
                    "CREATE INDEX IF NOT EXISTS idx_movies_api_id          ON movies(api_id)",
                    "CREATE INDEX IF NOT EXISTS idx_movies_is_new          ON movies(is_new)",
                    "CREATE INDEX IF NOT EXISTS idx_movies_year            ON movies(year)",
                    
                    "CREATE INDEX IF NOT EXISTS idx_episodes_url           ON episodes(url)",
                    "CREATE INDEX IF NOT EXISTS idx_episodes_show_url      ON episodes(show_url)",
                    "CREATE INDEX IF NOT EXISTS idx_episodes_api_id        ON episodes(api_id)",
                    "CREATE INDEX IF NOT EXISTS idx_episodes_is_new        ON episodes(is_new)",
                    "CREATE INDEX IF NOT EXISTS idx_episodes_modified      ON episodes(is_modified)",
                    "CREATE INDEX IF NOT EXISTS idx_episodes_season_ep     ON episodes(season_number, episode_number)",
                    "CREATE INDEX IF NOT EXISTS idx_episodes_show_season_ep ON episodes(show_url, season_number, episode_number)",
                    "CREATE INDEX IF NOT EXISTS idx_episodes_show_id       ON episodes(show_id)"
                ]:
                    self.cursor.execute(ddl)

                self.logger.info("Database tables and indexes created successfully")
                
            except sqlite3.Error as e:
                self.logger.error(f"Error creating tables: {e}")
                raise

    def _ensure_columns(self) -> None:
        """Check for any columns missing from older DBs and add them."""
        REQUIRED: Dict[str, List[tuple[str, str]]] = {
            "movies": [
                ("api_id", "INTEGER"),
                ("api_source", "TEXT"),
                ("modified_gmt", "TEXT"),
                ("language_code", "TEXT DEFAULT 'fa'")
            ],
            "shows": [
                ("episode_urls", "TEXT"),
                ("api_id", "INTEGER"),
                ("api_source", "TEXT"),
                ("modified_gmt", "TEXT"),
                ("language_code", "TEXT DEFAULT 'fa'"),
                ("is_modified", "INTEGER DEFAULT 0"),
                ("last_aired", "TEXT")
            ],
            "episodes": [
                ("api_id", "INTEGER"),
                ("api_source", "TEXT"),
                ("modified_gmt", "TEXT"),
                ("show_id", "INTEGER"),
                ("language_code", "TEXT DEFAULT 'fa'"),
                ("is_modified", "INTEGER DEFAULT 0")
            ]
        }

        try:
            with SafeTransactionManager(self.conn, self.logger) as _:
                for table, cols in REQUIRED.items():
                    existing_cols = {row[1] for row in self.cursor.execute(f"PRAGMA table_info({table})")}
                    for col_name, col_type in cols:
                        if col_name not in existing_cols:
                            self.logger.info(f"Adding missing column {col_name} to {table}")
                            self.cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
                            
                self.logger.info("Column migrations completed successfully")
                
        except sqlite3.Error as e:
            self.logger.error(f"Column migration error: {e}")
            raise

    def _sanitize_params(self, params: Any) -> Any:
        """
        Sanitize parameters for SQL queries to prevent SQL injection.
        
        Args:
            params: Parameters to sanitize (single value, tuple, list, or dict)
            
        Returns:
            Sanitized parameters
        """
        if params is None:
            return None
            
        # If it's a dict, sanitize each value
        if isinstance(params, dict):
            return {k: self._sanitize_param(v) for k, v in params.items()}
            
        # If it's a sequence (list, tuple), sanitize each element
        if isinstance(params, (list, tuple)):
            return [self._sanitize_param(p) for p in params]
            
        # Single value
        return self._sanitize_param(params)
        
    def _sanitize_param(self, value: Any) -> Any:
        """
        Sanitize a single parameter value.
        
        Args:
            value: Value to sanitize
            
        Returns:
            Sanitized value
        """
        # None passes through unchanged
        if value is None:
            return None
            
        # Basic scalar types are safe
        if isinstance(value, (int, float, bool)):
            return value
            
        # For strings, ensure they're properly encoded and don't contain null bytes
        if isinstance(value, str):
            # Replace null bytes with spaces (SQLite doesn't like null bytes)
            return value.replace('\0', ' ')
            
        # For complex types (lists, dicts), convert to JSON string
        if isinstance(value, (list, dict, set)):
            return json.dumps(value, ensure_ascii=False)
            
        # For date/time objects, convert to ISO format
        if isinstance(value, datetime):
            return value.isoformat()
            
        # For all other types, convert to string
        return str(value)
        
    def _validate_query_structure(self, query: str) -> None:
        """
        Validate query structure to prevent potentially dangerous operations.
        
        Args:
            query: SQL query to validate
            
        Raises:
            ValueError: If query is empty, not a string, or contains dangerous keywords
        """
        if not query or not isinstance(query, str):
            raise ValueError("Invalid SQL query")
            
        # Check for potentially dangerous operations
        # This is a basic check and not foolproof - parameterization is still essential
        normalized_query = query.upper()
        dangerous_keywords = [
            'DROP TABLE', 'DROP DATABASE', 
            'DELETE FROM', 'TRUNCATE TABLE',
            'ALTER TABLE', 'ATTACH DATABASE', 
            'DETACH DATABASE'
        ]
        
        # Look for standalone keywords (surrounded by spaces or at start/end)
        for keyword in dangerous_keywords:
            pattern = f" {keyword} "
            if (pattern in f" {normalized_query} " or 
                normalized_query.startswith(f"{keyword} ") or 
                normalized_query.endswith(f" {keyword}")):
                raise ValueError(f"Potentially dangerous SQL operation: {keyword}")

    def execute(self, query: str, params: Any = None) -> sqlite3.Cursor:
        """
        Execute a SQL query with proper error handling and parameter sanitization.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            SQLite cursor object
            
        Raises:
            sqlite3.Error: If query execution fails
        """
        with self._lock:
            try:
                # Validate query structure
                self._validate_query_structure(query)
                
                # Execute with retry logic
                max_retries = 3
                retry_delay = 1.0
                
                for attempt in range(max_retries):
                    try:
                        # Only pass parameters if they exist
                        if params is None:
                            return self.cursor.execute(query)
                        else:
                            sanitized_params = self._sanitize_params(params)
                            return self.cursor.execute(query, sanitized_params)
                    except sqlite3.OperationalError as e:
                        # Only retry on database locked errors
                        if 'database is locked' in str(e) or 'database is busy' in str(e):
                            if attempt < max_retries - 1:
                                backoff_time = retry_delay * (2 ** attempt)
                                jitter = random.uniform(0.8, 1.2)
                                sleep_time = backoff_time * jitter
                                
                                self.logger.warning(f"Database locked/busy, retrying in {sleep_time:.2f}s (attempt {attempt+1}/{max_retries})")
                                time.sleep(sleep_time)
                            else:
                                # Re-raise on last attempt
                                self.logger.error(f"Database still locked after {max_retries} attempts: {e}")
                                raise
                        else:
                            # Re-raise other operational errors immediately
                            raise
                
                # This should not be reached due to the raise in the loop
                raise sqlite3.OperationalError("Query execution failed after multiple attempts")
                
            except sqlite3.Error as e:
                self.logger.error(f"SQL execution error: {e}\nQuery: {query}\nParams: {params}")
                raise

    def executemany(self, query: str, params_list: List[Any]) -> sqlite3.Cursor:
        """
        Execute a SQL query with multiple parameter sets.
        
        Args:
            query: SQL query to execute
            params_list: List of parameter sets
            
        Returns:
            SQLite cursor object
            
        Raises:
            sqlite3.Error: If query execution fails
        """
        with self._lock:
            try:
                # Validate query structure
                self._validate_query_structure(query)
                
                # Sanitize parameters
                sanitized_params_list = [self._sanitize_params(params) for params in params_list]
                
                # Execute with retry logic
                max_retries = 3
                retry_delay = 1.0
                
                for attempt in range(max_retries):
                    try:
                        return self.cursor.executemany(query, sanitized_params_list)
                    except sqlite3.OperationalError as e:
                        # Only retry on database locked errors
                        if 'database is locked' in str(e) or 'database is busy' in str(e):
                            if attempt < max_retries - 1:
                                backoff_time = retry_delay * (2 ** attempt)
                                jitter = random.uniform(0.8, 1.2)
                                sleep_time = backoff_time * jitter
                                
                                self.logger.warning(f"Database locked/busy, retrying in {sleep_time:.2f}s (attempt {attempt+1}/{max_retries})")
                                time.sleep(sleep_time)
                            else:
                                # Re-raise on last attempt
                                self.logger.error(f"Database still locked after {max_retries} attempts: {e}")
                                raise
                        else:
                            # Re-raise other operational errors immediately
                            raise
                
                # This should not be reached due to the raise in the loop
                raise sqlite3.OperationalError("Query execution failed after multiple attempts")
                
            except sqlite3.Error as e:
                self.logger.error(f"SQL executemany error: {e}\nQuery: {query}")
                raise

    def fetchall(self, query: str, params: Any = None) -> List[sqlite3.Row]:
        """
        Execute a query and fetch all results with proper error handling.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            List of SQLite Row objects
            
        Raises:
            sqlite3.Error: If query execution fails
        """
        with self._lock:
            try:
                cursor = self.execute(query, params)
                return cursor.fetchall()
            except sqlite3.Error as e:
                self.logger.error(f"Error in fetchall: {e}\nQuery: {query}\nParams: {params}")
                raise

    def fetchone(self, query: str, params: Any = None) -> Optional[sqlite3.Row]:
        """
        Execute a query and fetch one result with proper error handling.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            SQLite Row object or None if no results
            
        Raises:
            sqlite3.Error: If query execution fails
        """
        with self._lock:
            try:
                cursor = self.execute(query, params)
                return cursor.fetchone()
            except sqlite3.Error as e:
                self.logger.error(f"Error in fetchone: {e}\nQuery: {query}\nParams: {params}")
                raise

    def commit(self) -> None:
        """Commit the current transaction with error handling."""
        with self._lock:
            try:
                self.conn.commit()
            except sqlite3.Error as e:
                self.logger.error(f"Error committing transaction: {e}")
                self.rollback()
                raise

    def rollback(self) -> None:
        """Roll back the current transaction with error handling."""
        with self._lock:
            try:
                self.conn.rollback()
            except sqlite3.Error as e:
                self.logger.error(f"Error rolling back transaction: {e}")
                raise

    def close(self) -> None:
        """Close the database connection and clean up resources."""
        with self._lock:
            try:
                # Return the connection to the pool
                if hasattr(self, 'connection_pool') and hasattr(self, 'conn'):
                    self.connection_pool.release_connection(self.conn)
                    self.conn = None
                    self.cursor = None
                
                # Close all connections in the pool
                if hasattr(self, 'connection_pool'):
                    self.connection_pool.close_all()
                    
                self.logger.info("Database connection closed")
                
            except Exception as e:
                self.logger.error(f"Error closing database connection: {e}")

    def optimize_database(self) -> bool:
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
            self.logger.info("Starting database optimization")
            
            # Run integrity check first
            check_result = self.fetchone("PRAGMA quick_check")
            if check_result and check_result[0] != "ok":
                self.logger.error(f"Database integrity check failed: {check_result[0]}")
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
            
            self.logger.info("Database optimization completed successfully")
            return True
        except Exception as e:
            self.logger.error(f"Database optimization failed: {e}")
            return False

    def get_related_episodes(self, show_url: str) -> List[Dict]:
        """
        Get all episodes related to a show.
        
        Args:
            show_url: URL of the show
            
        Returns:
            List of episode dictionaries
        """
        try:
            with self._lock:
                # Normalize URL for consistent matching
                show_url = show_url.rstrip('/')
                
                # Use LIKE to handle URL format inconsistencies
                episodes = self.fetchall(
                    "SELECT * FROM episodes WHERE show_url LIKE ? ORDER BY season_number, episode_number", 
                    [f"{show_url}%"]
                )
                
                result = []
                for episode in episodes:
                    ep_dict = dict(episode)
                    for field in ['video_files']:
                        if field in ep_dict and ep_dict[field]:
                            try:
                                ep_dict[field] = json.loads(ep_dict[field])
                            except json.JSONDecodeError:
                                self.logger.warning(f"Failed to parse JSON in {field} for episode {ep_dict.get('url')}")
                                ep_dict[field] = []
                    result.append(ep_dict)
                return result
        except sqlite3.Error as e:
            self.logger.error(f"Error getting related episodes: {e}")
            return []

    def mark_content_as_processed(self, content_type: str, ids: List[int]) -> bool:
        """
        Mark content items as processed (is_new=0) with thread safety.
        
        Args:
            content_type: Type of content ('shows', 'episodes', 'movies')
            ids: List of item IDs
            
        Returns:
            True if successful, False otherwise
        """
        if not ids:
            return True
            
        try:
            # Map content types to table names
            table_map = {
                'shows': 'shows',
                'series': 'shows',  # Allow 'series' to map to 'shows' table
                'episodes': 'episodes',
                'movies': 'movies'
            }
            
            table = table_map.get(content_type)
            if not table:
                self.logger.error(f"Invalid content type: {content_type}")
                return False
                
            with self._lock:
                with SafeTransactionManager(self.conn, self.logger) as _:
                    # Prepare placeholders for the IN clause
                    placeholders = ', '.join(['?'] * len(ids))
                    
                    # Update the records with thread safety
                    query = f"UPDATE {table} SET is_new = 0 WHERE id IN ({placeholders})"
                    self.execute(query, ids)
                    
                    self.logger.info(f"Marked {len(ids)} {content_type} as processed")
                    return True
        except sqlite3.Error as e:
            self.logger.error(f"Error marking content as processed: {e}")
            return False
            
    def mark_content_as_modified(self, content_type: str, ids: List[int], modified: bool = True) -> bool:
        """
        Mark content items as modified or not modified.
        
        Args:
            content_type: Type of content ('shows', 'episodes', 'movies')
            ids: List of item IDs
            modified: Whether to mark as modified (True) or not modified (False)
            
        Returns:
            True if successful, False otherwise
        """
        if not ids:
            return True
            
        try:
            # Map content types to table names
            table_map = {
                'shows': 'shows',
                'series': 'shows',  # Allow 'series' to map to 'shows' table
                'episodes': 'episodes',
                'movies': 'movies'
            }
            
            table = table_map.get(content_type)
            if not table:
                self.logger.error(f"Invalid content type: {content_type}")
                return False
                
            # Check if table has is_modified column
            has_modified = False
            for row in self.fetchall(f"PRAGMA table_info({table})"):
                if row[1] == 'is_modified':
                    has_modified = True
                    break
                    
            if not has_modified:
                self.logger.warning(f"Table {table} does not have is_modified column, skipping")
                return False
                
            with self._lock:
                with SafeTransactionManager(self.conn, self.logger) as _:
                    # Prepare placeholders for the IN clause
                    placeholders = ', '.join(['?'] * len(ids))
                    
                    # Update the records
                    modified_value = 1 if modified else 0
                    query = f"UPDATE {table} SET is_modified = {modified_value} WHERE id IN ({placeholders})"
                    self.execute(query, ids)
                    
                    self.logger.info(f"Marked {len(ids)} {content_type} as {'modified' if modified else 'not modified'}")
                    return True
        except sqlite3.Error as e:
            self.logger.error(f"Error marking content as {'modified' if modified else 'not modified'}: {e}")
            return False
            
    def _update_show_episode_count(self, show_url: str) -> bool:
        """
        Update the episode count for a show based on actual episodes in database.
        
        Args:
            show_url: The URL of the show to update
            
        Returns:
            True if successful, False otherwise
        """
        if not self.conn or not show_url:
            return False
            
        try:
            # Normalize URL for better matching
            show_url = show_url.rstrip('/')
            
            with self._lock:
                with SafeTransactionManager(self.conn, self.logger) as _:
                    # Use LIKE to handle URL format inconsistencies
                    result = self.fetchone(
                        "SELECT COUNT(*) as count FROM episodes WHERE show_url LIKE ?",
                        [f"{show_url}%"]
                    )
                    
                    if not result:
                        self.logger.warning(f"Failed to count episodes for {show_url}")
                        return False
                        
                    episode_count = result["count"]
                    
                    # Update the show with the new count - try exact match first
                    updated = self.execute(
                        "UPDATE shows SET episode_count=? WHERE url=?",
                        (episode_count, show_url)
                    ).rowcount
                    
                    # If exact match didn't work, try with LIKE
                    if updated == 0:
                        updated = self.execute(
                            "UPDATE shows SET episode_count=? WHERE url LIKE ?",
                            (episode_count, f"{show_url}%")
                        ).rowcount
                        
                    if updated > 0:
                        self.logger.debug(f"Updated episode count for {show_url}: {episode_count}")
                        return True
                    else:
                        self.logger.warning(f"No show found for URL: {show_url}")
                        return False
                        
        except Exception as e:
            self.logger.error(f"Error updating episode count for {show_url}: {e}")
            return False
            
    def atomic_write_json(self, data: Any, output_path: str) -> bool:
        """
        Write data to a JSON file using atomic file operations.
        
        Args:
            data: Data to write
            output_path: Path to write to
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Create temp file in same directory for atomic move
            fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(output_path), suffix='.json.tmp')
            
            try:
                # Write to temp file
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                    # Ensure data is flushed to disk
                    f.flush()
                    os.fsync(f.fileno())
                    
                # Atomic replace
                os.replace(temp_path, output_path)
                return True
                
            except Exception as e:
                # Clean up temp file on error
                self.logger.error(f"Error writing file: {e}")
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                return False
                
        except Exception as e:
            self.logger.error(f"Error in atomic_write_json: {e}")
            return False

    def export_to_json(self, output_path: Optional[str] = None, pretty: bool = True) -> bool:
        """
        Export database contents to a JSON file.
        
        Args:
            output_path: Path to save the JSON file
            pretty: Whether to format the JSON with indentation
            
        Returns:
            True if successful, False otherwise
        """
        if output_path is None:
            output_path = str(JSON_OUTPUT_PATH)

        try:
            # Ensure output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Prepare metadata
            data = {
                "metadata": {
                    "exported_at": datetime.now().isoformat(),
                    "version": "5.0.0"
                }
            }

            # Count records in each table
            for table in ['movies', 'shows', 'episodes']:
                count = self.fetchone(f"SELECT COUNT(*) as count FROM {table}")
                data[f"{table}_count"] = count['count'] if count else 0

            # Load and process data from each table
            data['movies'] = self._load_table_with_json_fields('movies', ['genres', 'directors', 'cast', 'video_files'])
            all_episodes = self._load_table_with_json_fields('episodes', ['video_files'])

            # Create a lookup map for episodes by show_url
            show_episodes_map = {}
            for ep in all_episodes:
                show_url = ep.get('show_url')
                if show_url:
                    # Standardize URLs for consistent matching
                    show_url = show_url.rstrip('/')
                    if show_url not in show_episodes_map:
                        show_episodes_map[show_url] = []
                    show_episodes_map[show_url].append(ep)

            # Process shows and add related episodes
            shows = self._load_table_with_json_fields('shows', ['genres', 'directors', 'cast', 'seasons'])
            for show in shows:
                # Standardize URL for matching
                show_url = show.get('url', '').rstrip('/')
                
                # Get episodes for this show
                related_eps = show_episodes_map.get(show_url, [])
                
                # Log relationship info for debugging
                self.logger.debug(f"Show: {show.get('title_en')} ({show_url}) has {len(related_eps)} related episodes")
                
                # Organize episodes by season
                seasons_map = {}
                for ep in related_eps:
                    season_num = ep.get('season_number', 0)
                    if season_num not in seasons_map:
                        seasons_map[season_num] = []
                    seasons_map[season_num].append(ep)
                    
                # Format seasons data
                final_seasons = []
                for season_num in sorted(seasons_map.keys()):
                    episodes = seasons_map[season_num]
                    
                    # Format episodes data
                    final_episodes = []
                    for ep in sorted(episodes, key=lambda x: x.get('episode_number', 0)):
                        final_episodes.append({
                            "episode_number": ep.get('episode_number'),
                            "title": ep.get('title'),
                            "date": ep.get('air_date'),
                            "url": ep.get('url'),
                            "thumbnail": ep.get('thumbnail'),
                            "lastmod": ep.get('lastmod'),
                            "video_files": ep.get('video_files', [])
                        })
                        
                    # Add season data
                    final_seasons.append({
                        "season_number": season_num,
                        "title": f"Season {season_num}",
                        "episode_count": len(final_episodes),
                        "episodes": final_episodes
                    })
                
                # If no seasons were created but we have original seasons data, try to use it
                if not final_seasons and show.get('seasons'):
                    try:
                        # If this is a string, parse it as JSON
                        if isinstance(show['seasons'], str):
                            seasons_data = json.loads(show['seasons'])
                            final_seasons = seasons_data
                            self.logger.debug(f"Using original seasons JSON data for {show.get('title_en')}")
                    except (json.JSONDecodeError, TypeError) as e:
                        self.logger.warning(f"Failed to parse original seasons data for {show.get('title_en')}: {e}")
                
                # Assign processed data to the show
                show['seasons'] = final_seasons
                show['full_episodes'] = related_eps

            # Save all data to the output file
            data['shows'] = shows
            data['episodes'] = all_episodes

            # Use atomic write to ensure data integrity
            return self.atomic_write_json(data, output_path)
                
        except Exception as e:
            self.logger.error(f"Error exporting to JSON: {e}", exc_info=True)
            return False

    def _load_table_with_json_fields(self, table: str, json_fields: List[str]) -> List[Dict]:
        """
        Load table data and parse JSON fields with improved error handling.
        
        Args:
            table: Table name
            json_fields: List of fields containing JSON data
            
        Returns:
            List of row dictionaries with parsed JSON fields
        """
        try:
            with self._lock:
                # Get all rows from the table with retry logic
                max_retries = 3
                retry_delay = 1.0
                rows = None
                
                for attempt in range(max_retries):
                    try:
                        rows = self.fetchall(f"SELECT * FROM {table}")
                        break
                    except sqlite3.OperationalError as e:
                        if 'database is locked' in str(e) or 'database is busy' in str(e):
                            if attempt < max_retries - 1:
                                backoff_time = retry_delay * (2 ** attempt)
                                jitter = random.uniform(0.8, 1.2)
                                sleep_time = backoff_time * jitter
                                
                                self.logger.warning(f"Database locked/busy, retrying in {sleep_time:.2f}s (attempt {attempt+1}/{max_retries})")
                                time.sleep(sleep_time)
                            else:
                                self.logger.error(f"Could not fetch data after {max_retries} attempts")
                                return []
                        else:
                            self.logger.error(f"Error fetching data: {e}")
                            return []
                
                if not rows:
                    return []
                    
                # Process each row with batch processing for memory efficiency
                result = []
                batch_size = 1000 if self.low_memory_mode else 5000
                
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    batch_result = []
                    
                    for row in batch:
                        # Convert sqlite3.Row to dict
                        row_dict = dict(row)
                        
                        # Parse JSON fields
                        for field in json_fields:
                            if field in row_dict and row_dict[field]:
                                try:
                                    if isinstance(row_dict[field], str):
                                        row_dict[field] = json.loads(row_dict[field])
                                except json.JSONDecodeError:
                                    self.logger.warning(f"Failed to parse JSON in {field} for {table} {row_dict.get('url', row_dict.get('id'))}")
                                    row_dict[field] = []
                                    
                        batch_result.append(row_dict)
                    
                    result.extend(batch_result)
                    
                    # Force garbage collection in low memory mode after each batch
                    if self.low_memory_mode:
                        gc.collect()
                    
                return result
                
        except sqlite3.Error as e:
            self.logger.error(f"Error loading table {table}: {e}")
            return []
    
    # Context manager support
    def __enter__(self) -> 'Database':
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager and close connection."""
        self.close()
        
    @property
    def low_memory_mode(self) -> bool:
        """Check if we should operate in low memory mode (Docker/NAS)."""
        return IN_DOCKER or self._detect_low_memory()
        
    def _detect_low_memory(self) -> bool:
        """Detect if system is running with limited memory."""
        if not PSUTIL_AVAILABLE:
            return False
            
        try:
            # Check available memory
            mem = psutil.virtual_memory()
            total_gb = mem.total / (1024**3)
            available_gb = mem.available / (1024**3)
            
            # Consider low memory if less than 2GB available or less than 4GB total
            return available_gb < 2.0 or total_gb < 4.0
        except Exception:
            return False
    
    # -------------------------------------------------------------------
    # Content manipulation methods
    # -------------------------------------------------------------------
    def add_show(self, show_data: Dict[str, Any]) -> int:
        """
        Add or update a show in the database.
        
        Args:
            show_data: Show data dictionary
            
        Returns:
            ID of the inserted/updated show or 0 if failed
        """
        if not show_data.get('url'):
            self.logger.error("Cannot add show without URL")
            return 0
            
        try:
            # Convert lists to JSON strings
            data = show_data.copy()
            for field in ['genres', 'directors', 'cast', 'seasons', 'episode_urls']:
                if field in data and isinstance(data[field], (list, dict)):
                    data[field] = json.dumps(data[field], ensure_ascii=False)
            
            with self._lock:
                with SafeTransactionManager(self.conn, self.logger) as _:
                    # Prepare column names and placeholders
                    columns = []
                    placeholders = []
                    values = []
                    
                    for key, value in data.items():
                        if key != 'id':  # Skip ID field
                            columns.append(key)
                            placeholders.append('?')
                            values.append(value)
                    
                    # Generate SQL (use INSERT OR REPLACE for update on duplicate)
                    sql = f"INSERT OR REPLACE INTO shows ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                    
                    # Execute with proper parameter sanitization
                    self.execute(sql, values)
                    
                    # Get ID of inserted/updated row
                    if 'id' in show_data and show_data['id']:
                        return show_data['id']
                    else:
                        row = self.fetchone("SELECT id FROM shows WHERE url = ?", (data['url'],))
                        return row['id'] if row else 0
                        
        except Exception as e:
            self.logger.error(f"Error adding show: {e}", exc_info=True)
            return 0
    
    def add_movie(self, movie_data: Dict[str, Any]) -> int:
        """
        Add or update a movie in the database.
        
        Args:
            movie_data: Movie data dictionary
            
        Returns:
            ID of the inserted/updated movie or 0 if failed
        """
        if not movie_data.get('url'):
            self.logger.error("Cannot add movie without URL")
            return 0
            
        try:
            # Convert lists to JSON strings
            data = movie_data.copy()
            for field in ['genres', 'directors', 'cast', 'video_files']:
                if field in data and isinstance(data[field], (list, dict)):
                    data[field] = json.dumps(data[field], ensure_ascii=False)
            
            with self._lock:
                with SafeTransactionManager(self.conn, self.logger) as _:
                    # Prepare column names and placeholders
                    columns = []
                    placeholders = []
                    values = []
                    
                    for key, value in data.items():
                        if key != 'id':  # Skip ID field
                            columns.append(key)
                            placeholders.append('?')
                            values.append(value)
                    
                    # Generate SQL (use INSERT OR REPLACE for update on duplicate)
                    sql = f"INSERT OR REPLACE INTO movies ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                    
                    # Execute with proper parameter sanitization
                    self.execute(sql, values)
                    
                    # Get ID of inserted/updated row
                    if 'id' in movie_data and movie_data['id']:
                        return movie_data['id']
                    else:
                        row = self.fetchone("SELECT id FROM movies WHERE url = ?", (data['url'],))
                        return row['id'] if row else 0
                        
        except Exception as e:
            self.logger.error(f"Error adding movie: {e}", exc_info=True)
            return 0
    
    def add_episode(self, episode_data: Dict[str, Any]) -> int:
        """
        Add or update an episode in the database.
        
        Args:
            episode_data: Episode data dictionary
            
        Returns:
            ID of the inserted/updated episode or 0 if failed
        """
        if not episode_data.get('url'):
            self.logger.error("Cannot add episode without URL")
            return 0
            
        try:
            # Convert lists to JSON strings
            data = episode_data.copy()
            for field in ['video_files']:
                if field in data and isinstance(data[field], (list, dict)):
                    data[field] = json.dumps(data[field], ensure_ascii=False)
            
            with self._lock:
                with SafeTransactionManager(self.conn, self.logger) as _:
                    # Prepare column names and placeholders
                    columns = []
                    placeholders = []
                    values = []
                    
                    for key, value in data.items():
                        if key != 'id':  # Skip ID field
                            columns.append(key)
                            placeholders.append('?')
                            values.append(value)
                    
                    # Generate SQL (use INSERT OR REPLACE for update on duplicate)
                    sql = f"INSERT OR REPLACE INTO episodes ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                    
                    # Execute with proper parameter sanitization
                    self.execute(sql, values)
                    
                    # Get ID of inserted/updated row
                    if 'id' in episode_data and episode_data['id']:
                        inserted_id = episode_data['id']
                    else:
                        row = self.fetchone("SELECT id FROM episodes WHERE url = ?", (data['url'],))
                        inserted_id = row['id'] if row else 0
                    
                    # Update show episode count if show URL is provided
                    if data.get('show_url'):
                        self._update_show_episode_count(data['show_url'])
                    
                    return inserted_id
                    
        except Exception as e:
            self.logger.error(f"Error adding episode: {e}", exc_info=True)
            return 0

    def get_new_items(self, content_type: str, limit: int = 100) -> List[Dict]:
        """
        Get items marked as new.
        
        Args:
            content_type: Type of content ('shows', 'episodes', 'movies')
            limit: Maximum number of items to return
            
        Returns:
            List of new items
        """
        # Map content types to table names
        table_map = {
            'shows': 'shows',
            'series': 'shows',
            'episodes': 'episodes',
            'movies': 'movies'
        }
        
        table = table_map.get(content_type)
        if not table:
            self.logger.error(f"Invalid content type: {content_type}")
            return []
            
        try:
            with self._lock:
                rows = self.fetchall(
                    f"SELECT * FROM {table} WHERE is_new = 1 LIMIT ?", 
                    (limit,)
                )
                
                result = []
                for row in rows:
                    row_dict = dict(row)
                    # Parse JSON fields if needed
                    if table == 'shows':
                        json_fields = ['genres', 'directors', 'cast', 'seasons', 'episode_urls']
                    elif table == 'episodes':
                        json_fields = ['video_files']
                    elif table == 'movies':
                        json_fields = ['genres', 'directors', 'cast', 'video_files']
                    else:
                        json_fields = []
                        
                    for field in json_fields:
                        if field in row_dict and row_dict[field]:
                            try:
                                if isinstance(row_dict[field], str):
                                    row_dict[field] = json.loads(row_dict[field])
                            except json.JSONDecodeError:
                                self.logger.warning(f"Failed to parse JSON in {field}")
                                row_dict[field] = []
                                
                    result.append(row_dict)
                    
                return result
        except sqlite3.Error as e:
            self.logger.error(f"Error getting new items: {e}")
            return []

    def get_modified_items(self, content_type: str, limit: int = 100) -> List[Dict]:
        """
        Get items marked as modified.
        
        Args:
            content_type: Type of content ('shows', 'episodes', 'movies')
            limit: Maximum number of items to return
            
        Returns:
            List of modified items
        """
        # Map content types to table names
        table_map = {
            'shows': 'shows',
            'series': 'shows',
            'episodes': 'episodes',
            'movies': 'movies'
        }
        
        table = table_map.get(content_type)
        if not table:
            self.logger.error(f"Invalid content type: {content_type}")
            return []
            
        try:
            with self._lock:
                # Check if table has is_modified column
                has_modified = False
                for row in self.fetchall(f"PRAGMA table_info({table})"):
                    if row[1] == 'is_modified':
                        has_modified = True
                        break
                        
                if not has_modified:
                    self.logger.warning(f"Table {table} does not have is_modified column, returning empty list")
                    return []
                    
                rows = self.fetchall(
                    f"SELECT * FROM {table} WHERE is_modified = 1 LIMIT ?", 
                    (limit,)
                )
                
                result = []
                for row in rows:
                    row_dict = dict(row)
                    # Parse JSON fields if needed
                    if table == 'shows':
                        json_fields = ['genres', 'directors', 'cast', 'seasons', 'episode_urls']
                    elif table == 'episodes':
                        json_fields = ['video_files']
                    elif table == 'movies':
                        json_fields = ['genres', 'directors', 'cast', 'video_files']
                    else:
                        json_fields = []
                        
                    for field in json_fields:
                        if field in row_dict and row_dict[field]:
                            try:
                                if isinstance(row_dict[field], str):
                                    row_dict[field] = json.loads(row_dict[field])
                            except json.JSONDecodeError:
                                self.logger.warning(f"Failed to parse JSON in {field}")
                                row_dict[field] = []
                                
                    result.append(row_dict)
                    
                return result
        except sqlite3.Error as e:
            self.logger.error(f"Error getting modified items: {e}")
            return []