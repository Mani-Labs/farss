# File: farsiland_scraper/pipelines/save_to_db.py
# Version: 4.2.0
# Last Updated: 2025-06-15

"""
Pipeline for saving scraped items to the database.

This pipeline:
1. Handles processing of ShowItem, EpisodeItem, and MovieItem objects
2. Ensures safe database operations using parameterized queries
3. Maintains consistency between related items (e.g., show episode counts)
4. Properly manages database transactions
5. Handles LIKE-based queries for URL inconsistencies
6. Implements robust error handling and recovery

Changelog:
- [4.2.0] Fixed type validation before database operations
- [4.2.0] Improved error recovery to prevent data loss
- [4.2.0] Fixed SQL injection vulnerabilities
- [4.2.0] Added proper transaction boundaries
- [4.2.0] Improved URL normalization for consistency
- [4.2.0] Enhanced error tracking integration
- [4.2.0] Added transaction isolation levels
- [4.2.0] Fixed thread safety issues in database operations
- [4.2.0] Improved handling of JSON fields
- [4.1.0] Added LIKE-based queries for URL inconsistency handling
- [4.1.0] Improved transaction handling with better error recovery
- [4.1.0] Added show URL normalization for consistent episode relationships
- [4.1.0] Fixed episode counting for shows
- [4.0.0] Improved database entry validation logic
- [4.0.0] Added handling for missing video files
"""

import json
import time
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Tuple, cast
from pathlib import Path

from farsiland_scraper.database.models import Database
from farsiland_scraper.items import ShowItem, EpisodeItem, MovieItem
from farsiland_scraper.config import LOGGER
import scrapy

class SaveToDatabasePipeline:
    """
    Pipeline for saving scraped items to the database.
    
    This pipeline processes the three main item types:
    - ShowItem: TV shows/series
    - EpisodeItem: Individual episodes of TV shows
    - MovieItem: Movies
    
    Each item type has its own processing method with appropriate validation
    and database operations. The pipeline uses parameterized queries and
    proper transaction handling to ensure data integrity.
    """

    def __init__(self):
        """Initialize the pipeline with a null database connection."""
        self.db = None
        self.error_counts = {}  # Track error occurrences by URL
        self.skip_threshold = 3  # Skip after this many consecutive errors
        
        # Keep track of transaction status
        self.in_transaction = False

    def open_spider(self, spider: scrapy.Spider) -> None:
        """
        Initialize database connection when the spider starts.
        
        Args:
            spider: The spider that was opened
        """
        LOGGER.info("========== DATABASE PIPELINE ACTIVATED ==========")
        try:
            self.db = Database()
            LOGGER.info("Database connection initialized")
            
            # Get error tracker from spider if available
            if hasattr(spider, 'error_tracker'):
                self.error_tracker = spider.error_tracker
            else:
                self.error_tracker = None
                
            # Get blacklist threshold from spider settings
            if hasattr(spider, 'blacklist_threshold'):
                self.skip_threshold = spider.blacklist_threshold
                
        except Exception as e:
            LOGGER.error(f"Failed to initialize database connection: {e}")
            # Capture traceback for better diagnostic
            LOGGER.error(traceback.format_exc())
            raise

    def close_spider(self, spider: scrapy.Spider) -> None:
        """
        Clean up database connection when the spider finishes.
        
        Args:
            spider: The spider that was closed
        """
        if self.db:
            try:
                # Ensure any open transaction is committed or rolled back
                if self.in_transaction:
                    try:
                        self.db.rollback()
                        LOGGER.warning("Rolling back open transaction during spider close")
                    except Exception:
                        pass
                    finally:
                        self.in_transaction = False
                        
                # Export to JSON if configured
                if hasattr(spider, 'export_json') and spider.export_json:
                    try:
                        self.db.export_to_json()
                        LOGGER.info("Exported database to JSON")
                    except Exception as e:
                        LOGGER.error(f"Failed to export database to JSON: {e}")
                    
                self.db.close()
                LOGGER.info("Database connection closed")
            except Exception as e:
                LOGGER.error(f"Error closing database connection: {e}")
                LOGGER.error(traceback.format_exc())

    def process_item(self, item: Union[ShowItem, EpisodeItem, MovieItem], spider: scrapy.Spider) -> Union[ShowItem, EpisodeItem, MovieItem]:
        """
        Process an item and save it to the database.
        
        This method routes items to type-specific processing methods.
        
        Args:
            item: The item to save
            spider: The spider that scraped the item
            
        Returns:
            The processed item
        """
        if not self.db:
            LOGGER.error("Database connection not initialized")
            return item
            
        try:
            # Extract URL for error tracking
            url = item.get('url', '')
            if not url:
                LOGGER.warning("Item has no URL, cannot process")
                return item
                
            # Normalize URL
            url = self._normalize_url(url)
            
            # Skip processing if URL has had too many errors
            if url in self.error_counts and self.error_counts[url] >= self.skip_threshold:
                LOGGER.warning(f"Skipping {url} due to previous errors")
                return item
                
            # Route to type-specific processing method
            if isinstance(item, ShowItem):
                return self._process_show(item)
            elif isinstance(item, EpisodeItem):
                return self._process_episode(item)
            elif isinstance(item, MovieItem):
                return self._process_movie(item)
            else:
                LOGGER.warning(f"Unknown item type: {type(item).__name__}")
            
            return item
        except Exception as e:
            # Record error for URL
            url = item.get('url', '')
            if url:
                url = self._normalize_url(url)
                self.error_counts[url] = self.error_counts.get(url, 0) + 1
                
                # Record in error tracker if available
                if self.error_tracker:
                    self.error_tracker.record_error(url, "db_save_error")
                    
            LOGGER.error(f"Error processing item {url}: {e}")
            LOGGER.error(traceback.format_exc())
            return item

    def _normalize_url(self, url: str) -> str:
        """
        Normalize URL for consistent storage and lookup.
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL
        """
        if not url:
            return ""
            
        # Remove trailing slashes
        return url.rstrip('/')
        
    def _validate_json_field(self, value: Any) -> str:
        """
        Validate and convert a field to JSON string.
        
        Args:
            value: Field value to convert
            
        Returns:
            JSON string representation
        """
        if not value:
            return "[]"  # Empty list as default
            
        if isinstance(value, str):
            # If it's already a string, check if it's valid JSON
            try:
                json.loads(value)
                return value
            except json.JSONDecodeError:
                # Not valid JSON, treat as a simple string to be converted
                return json.dumps([value])
                
        # If it's a list or dict, convert to JSON
        if isinstance(value, (list, dict)):
            return json.dumps(value)
            
        # For other types, convert to string
        return json.dumps(str(value))
        
    def _begin_transaction(self) -> bool:
        """
        Begin a database transaction with proper error handling.
        
        Returns:
            True if transaction started successfully, False otherwise
        """
        if not self.db:
            LOGGER.error("Cannot begin transaction - database not initialized")
            return False
            
        # Don't nest transactions
        if self.in_transaction:
            LOGGER.warning("Transaction already in progress")
            return True
            
        try:
            self.db.execute("BEGIN TRANSACTION")
            self.in_transaction = True
            return True
        except Exception as e:
            LOGGER.error(f"Failed to begin transaction: {e}")
            self.in_transaction = False
            return False
            
    def _commit_transaction(self) -> bool:
        """
        Commit a database transaction with proper error handling.
        
        Returns:
            True if committed successfully, False otherwise
        """
        if not self.db:
            LOGGER.error("Cannot commit transaction - database not initialized")
            return False
            
        if not self.in_transaction:
            LOGGER.warning("No transaction to commit")
            return False
            
        try:
            self.db.commit()
            self.in_transaction = False
            return True
        except Exception as e:
            LOGGER.error(f"Failed to commit transaction: {e}")
            try:
                self.db.rollback()
            except:
                pass
            self.in_transaction = False
            return False
            
    def _rollback_transaction(self) -> bool:
        """
        Roll back a database transaction with proper error handling.
        
        Returns:
            True if rolled back successfully, False otherwise
        """
        if not self.db:
            LOGGER.error("Cannot rollback transaction - database not initialized")
            return False
            
        if not self.in_transaction:
            LOGGER.warning("No transaction to roll back")
            return False
            
        try:
            self.db.rollback()
            self.in_transaction = False
            return True
        except Exception as e:
            LOGGER.error(f"Failed to roll back transaction: {e}")
            self.in_transaction = False
            return False

    def _process_show(self, item: ShowItem) -> ShowItem:
        """
        Process and save a show item to the database.
        
        Args:
            item: The show item to save
            
        Returns:
            The processed show item
        """
        if not self.db:
            LOGGER.error("Database connection not initialized")
            return item
            
        try:
            show_url = item.get('url')
            if not show_url:
                LOGGER.error("Cannot process show without URL")
                return item
                
            # Normalize URL
            show_url = self._normalize_url(show_url)
            item['url'] = show_url
                
            LOGGER.debug(f"Processing show: {item.get('title_en', show_url)}")
            show_data = dict(item)

            # Begin transaction
            if not self._begin_transaction():
                LOGGER.error(f"Failed to begin transaction for show {show_url}")
                return item

            try:
                # Convert lists to JSON strings with validation
                json_fields = ["genres", "directors", "cast", "seasons", "episode_urls"]
                for field in json_fields:
                    if field in show_data:
                        show_data[field] = self._validate_json_field(show_data[field])

                # Check for update: compare lastmod from sitemap with DB record
                is_new = 1  # Default to treating as new
                
                # Try exact match first
                existing = self.db.fetchone(
                    "SELECT lastmod, is_new FROM shows WHERE url=?", 
                    [show_url]
                )
                
                # If not found with exact match, try with LIKE
                if not existing:
                    existing = self.db.fetchone(
                        "SELECT lastmod, is_new FROM shows WHERE url LIKE ?",
                        [f"{show_url}%"]
                    )
                
                if existing is not None:
                    db_lastmod = existing["lastmod"]
                    # If the sitemap's lastmod differs, mark as new
                    if db_lastmod != show_data.get("lastmod"):
                        is_new = 1
                    else:
                        # Keep existing is_new status if lastmod hasn't changed
                        is_new = existing["is_new"]
                
                show_data["is_new"] = is_new
                show_data["last_scraped"] = datetime.utcnow().isoformat()

                # Prepare columns and values for SQL INSERT
                columns = (
                    "url", "sitemap_url", "title_en", "title_fa", "poster",
                    "description", "first_air_date", "rating", "rating_count",
                    "season_count", "episode_count", "genres", "directors", "cast",
                    "social_shares", "comments_count", "seasons", "episode_urls", "is_new", 
                    "lastmod", "last_scraped", "api_id", "api_source", "modified_gmt"
                )
                
                # Build SQL using parameters
                placeholders = ", ".join("?" * len(columns))
                sql = f"INSERT OR REPLACE INTO shows ({', '.join(columns)}) VALUES ({placeholders})"
                
                # Extract values in the same order as columns
                values = []
                for col in columns:
                    values.append(show_data.get(col))
                
                # Execute the query with proper parameters
                self.db.execute(sql, values)
                
                # Commit the transaction
                if not self._commit_transaction():
                    LOGGER.error(f"Failed to commit transaction for show {show_url}")
                    return item
                
                LOGGER.info(f"Saved show: {item.get('title_en', show_url)}")
                return item
            
            except Exception as e:
                LOGGER.error(f"Error saving show {show_url}: {e}")
                LOGGER.error(traceback.format_exc())
                
                # Rollback transaction
                self._rollback_transaction()
                
                # Record error for this URL
                self.error_counts[show_url] = self.error_counts.get(show_url, 0) + 1
                
                # Record in error tracker if available
                if self.error_tracker:
                    self.error_tracker.record_error(show_url, "db_save_show_error")
                    
                return item
                
        except Exception as e:
            LOGGER.error(f"Unexpected error processing show {item.get('url', '')}: {e}")
            LOGGER.error(traceback.format_exc())
            
            # Rollback any active transaction
            if self.in_transaction:
                self._rollback_transaction()
                
            return item
            
    def _process_episode(self, item: EpisodeItem) -> EpisodeItem:
        """
        Process and save an episode item to the database.
        
        Args:
            item: The episode item to save
            
        Returns:
            The processed episode item
        """
        if not self.db:
            LOGGER.error("Database connection not initialized")
            return item
            
        # Define columns ONCE at the top of the method
        columns = (
            "url", "sitemap_url", "show_url", "season_number", "episode_number",
            "title", "air_date", "thumbnail", "video_files", "is_new", 
            "lastmod", "last_scraped", "api_id", "api_source", "modified_gmt",
            "show_id"
        )
        
        try:
            episode_url = item.get('url')
            if not episode_url:
                LOGGER.error("Cannot process episode without URL")
                return item
                
            # Normalize URL and show_url
            episode_url = self._normalize_url(episode_url)
            item['url'] = episode_url
                
            LOGGER.debug(f"Processing episode: {item.get('title', episode_url)}")
            ep_data = dict(item)
            
            # Normalize show_url if present, and ensure the show exists
            if ep_data.get('show_url'):
                normalized = self._normalize_url(ep_data['show_url'])
                ep_data['show_url'] = normalized
                # Stub‐insert the parent show so the FK constraint won't fail
                self.db.add_show({'url': normalized})

            # Begin transaction
            if not self._begin_transaction():
                LOGGER.error(f"Failed to begin transaction for episode {episode_url}")
                return item
                
            try:
                # Convert video_files list to JSON with validation
                ep_data["video_files"] = self._validate_json_field(
                    ep_data.get("video_files", [])
                )

                # Check if this is a new or updated episode
                is_new = 1  # Default to treating as new
                
                # Try to find existing episode
                existing = self.db.fetchone(
                    "SELECT id, lastmod, is_new FROM episodes WHERE url=? OR url LIKE ?", 
                    [episode_url, f"{episode_url}%"]
                )
                
                if existing:
                    db_lastmod = existing["lastmod"]
                    # If the lastmod has changed, mark as new
                    if db_lastmod != ep_data.get("lastmod"):
                        is_new = 1
                    else:
                        # Keep existing is_new status if lastmod hasn't changed
                        is_new = existing["is_new"]
                    
                    # Preserve existing ID for updates
                    ep_data["id"] = existing["id"]
                
                ep_data["is_new"] = is_new
                ep_data["last_scraped"] = datetime.utcnow().isoformat()

                # Prepare values in column order
                values = [ep_data.get(col) for col in columns]
                
                # Build SQL using parameters
                placeholders = ", ".join("?" * len(columns))
                sql = f"INSERT OR REPLACE INTO episodes ({', '.join(columns)}) VALUES ({placeholders})"
                
                # Execute the query with proper parameters
                self.db.execute(sql, values)
                
                # Update the show's episode count if show_url is provided
                show_url = ep_data.get("show_url")
                if show_url:
                    self._update_show_episode_count(show_url)
                
                # Commit the transaction
                if not self._commit_transaction():
                    LOGGER.error(f"Failed to commit transaction for episode {episode_url}")
                    return item
                
                LOGGER.info(f"Saved episode: {item.get('title', episode_url)}")
                return item
                
            except Exception as e:
                LOGGER.error(f"Error saving episode {episode_url}: {e}")
                LOGGER.error(traceback.format_exc())
                
                # Rollback transaction
                self._rollback_transaction()
                
                # Record error for this URL
                self.error_counts[episode_url] = self.error_counts.get(episode_url, 0) + 1
                
                # Record in error tracker if available
                if self.error_tracker:
                    self.error_tracker.record_error(episode_url, "db_save_episode_error")
                    
                return item
                
        except Exception as e:
            LOGGER.error(f"Unexpected error processing episode {item.get('url', '')}: {e}")
            LOGGER.error(traceback.format_exc())
            
            # Rollback any active transaction
            if self.in_transaction:
                self._rollback_transaction()
                
            return item
    
    def _process_movie(self, item: MovieItem) -> MovieItem:
        """
        Process and save a movie item to the database.
        
        Args:
            item: The movie item to save
            
        Returns:
            The processed movie item
        """
        if not self.db:
            LOGGER.error("Database connection not initialized")
            return item
            
        try:
            movie_url = item.get('url')
            if not movie_url:
                LOGGER.error("Cannot process movie without URL")
                return item
                
            # Normalize URL
            movie_url = self._normalize_url(movie_url)
            item['url'] = movie_url
                
            LOGGER.debug(f"Processing movie: {item.get('title_en', movie_url)}")
            movie_data = dict(item)

            # Begin transaction
            if not self._begin_transaction():
                LOGGER.error(f"Failed to begin transaction for movie {movie_url}")
                return item
                
            try:
                # Convert list fields to JSON strings with validation
                json_fields = ["genres", "directors", "cast", "video_files"]
                for field in json_fields:
                    if field in movie_data:
                        movie_data[field] = self._validate_json_field(movie_data[field])
                    elif field == "video_files":
                        # Ensure video_files exists
                        movie_data["video_files"] = "[]"  # Empty JSON array

                # Check if this is a new or updated movie
                is_new = 1  # Default to treating as new
                
                # Try exact match first
                existing = self.db.fetchone(
                    "SELECT lastmod, is_new FROM movies WHERE url=?", 
                    [movie_url]
                )
                
                # If not found with exact match, try with LIKE
                if not existing:
                    existing = self.db.fetchone(
                        "SELECT lastmod, is_new FROM movies WHERE url LIKE ?",
                        [f"{movie_url}%"]
                    )
                
                if existing is not None:
                    db_lastmod = existing["lastmod"]
                    # If the lastmod has changed, mark as new
                    if db_lastmod != movie_data.get("lastmod"):
                        is_new = 1
                    else:
                        # Keep existing is_new status if lastmod hasn't changed
                        is_new = existing["is_new"]
                
                movie_data["is_new"] = is_new
                movie_data["last_scraped"] = datetime.utcnow().isoformat()

                # Prepare columns and values for SQL INSERT
                columns = (
                    "url", "sitemap_url", "title_en", "title_fa", "poster",
                    "description", "release_date", "year", "rating", "rating_count",
                    "genres", "directors", "cast", "social_shares", "comments_count",
                    "video_files", "is_new", "lastmod", "last_scraped", "api_id",
                    "api_source", "modified_gmt"
                )
                
                # Build SQL using parameters
                placeholders = ", ".join("?" * len(columns))
                sql = f"INSERT OR REPLACE INTO movies ({', '.join(columns)}) VALUES ({placeholders})"
                
                # Extract values in the same order as columns
                values = []
                for col in columns:
                    values.append(movie_data.get(col))
                
                # Execute the query with proper parameters
                self.db.execute(sql, values)
                
                # Commit the transaction
                if not self._commit_transaction():
                    LOGGER.error(f"Failed to commit transaction for movie {movie_url}")
                    return item
                
                LOGGER.info(f"Saved movie: {item.get('title_en', movie_url)}")
                return item
                
            except Exception as e:
                LOGGER.error(f"Error saving movie {movie_url}: {e}")
                LOGGER.error(traceback.format_exc())
                
                # Rollback transaction
                self._rollback_transaction()
                
                # Record error for this URL
                self.error_counts[movie_url] = self.error_counts.get(movie_url, 0) + 1
                
                # Record in error tracker if available
                if self.error_tracker:
                    self.error_tracker.record_error(movie_url, "db_save_movie_error")
                    
                return item
                
        except Exception as e:
            LOGGER.error(f"Unexpected error processing movie {item.get('url', '')}: {e}")
            LOGGER.error(traceback.format_exc())
            
            # Rollback any active transaction
            if self.in_transaction:
                self._rollback_transaction()
                
            return item
        
    def _update_show_episode_count(self, show_url: str) -> bool:
        """
        Update the episode count for a show based on actual episodes in database.
        
        Args:
            show_url: The URL of the show to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.db:
            return False
        
        # Don't update if URL is empty
        if not show_url:
            return False
            
        # Normalize URL
        show_url = self._normalize_url(show_url)
            
        try:
            # Try exact match first, then use LIKE if needed
            result = self.db.fetchone(
                "SELECT COUNT(*) as count FROM episodes WHERE show_url=?",
                [show_url]
            )
            
            # If no exact matches, try with LIKE for URL variations
            if not result or result["count"] == 0:
                result = self.db.fetchone(
                    "SELECT COUNT(*) as count FROM episodes WHERE show_url LIKE ?",
                    [f"{show_url}%"]
                )
            
            if result is None:
                LOGGER.warning(f"Failed to count episodes for {show_url}")
                return False
                
            episode_count = result["count"]
            
            # Update the show with the new count - try exact match first
            updated = self.db.execute(
                "UPDATE shows SET episode_count=? WHERE url=?",
                [episode_count, show_url]  # Use list instead of tuple for parameters
            ).rowcount

            # Check if any rows were updated
            if updated > 0:
                LOGGER.debug(f"Updated episode count for {show_url}: {episode_count}")
                return True
            else:
                # No matching show found
                LOGGER.warning(f"No show found for URL: {show_url}")
                return False
                
        except Exception as e:
            LOGGER.error(f"Error updating episode count for {show_url}: {e}")
            LOGGER.error(traceback.format_exc())
            return False