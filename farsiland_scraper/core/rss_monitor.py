# File: farsiland_scraper/core/rss_monitor.py
# Version: 1.1.0
# Last Updated: 2025-05-25

"""
RSS feed monitor for Farsiland scraper.

This module:
1. Monitors RSS feeds for new content
2. Tracks already processed items
3. Extracts content IDs from feed entries
4. Persists state between runs
5. Works with error tracking to skip problematic resources

Changelog:
- [1.1.0] Added integration with error tracker
- [1.1.0] Skip known failing resources
- [1.1.0] Added better error handling for RSS parsing
- [1.1.0] Improved state file handling with atomic operations
- [1.0.0] Initial implementation
"""

import os
import json
import time
import re
import logging
import tempfile
from datetime import datetime
from typing import Dict, List, Any, Optional

# Need to install this dependency: pip install feedparser
import feedparser

from farsiland_scraper.config import (
    RSS_FEEDS,
    RSS_STATE_FILE,
    RSS_POLL_INTERVAL,
    LOGGER
)

class RSSMonitor:
    """
    Monitor RSS feeds for new content.
    
    Tracks which items have been seen to avoid duplicates.
    Extracts content IDs from feed entries for API use.
    Integrates with error tracker to skip problematic resources.
    """
    
    def __init__(self, feeds=None, state_file=None, api_client=None, error_tracker=None):
        """
        Initialize RSS monitor.
        
        Args:
            feeds: Dict mapping feed types to URLs (default: from config)
            state_file: Path to state file (default: from config)
            api_client: Optional API client for ID extraction
            error_tracker: Optional error tracker instance
        """
        self.feeds = feeds or RSS_FEEDS
        self.state_file = state_file or RSS_STATE_FILE
        self.api_client = api_client
        self.error_tracker = error_tracker
        self.logger = LOGGER
        
        # Load or initialize state
        self.state = self.load_state()
        
    def load_state(self):
        """
        Load state from file or initialize new state.
        
        Returns:
            State dict with last seen GUIDs for each feed
        """
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                self.logger.error(f"Error loading state file: {e}")
                
                # Record error in tracker if available
                if self.error_tracker:
                    self.error_tracker.record_error(self.state_file, "rss_state_load_error")
        
        # Initialize empty state
        return {feed_type: {"last_guid": None, "last_check": None} 
                for feed_type in self.feeds}
                
    def save_state(self):
        """Save state to file using atomic write pattern."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            
            # Use atomic write pattern with temp file
            fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(self.state_file))
            try:
                with os.fdopen(fd, 'w') as f:
                    json.dump(self.state, f, indent=2)
                # Atomic replace
                os.replace(tmp_path, self.state_file)
            except Exception as e:
                # Clean up temp file on error
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise e
                
            self.logger.debug(f"State saved to {self.state_file}")
        except IOError as e:
            self.logger.error(f"Error saving state: {e}")
            
            # Record error in tracker if available
            if self.error_tracker:
                self.error_tracker.record_error(self.state_file, "rss_state_save_error")
        
    def check_for_updates(self):
        """
        Check all feeds for updates.
        
        Returns:
            Dict with new items by feed type
        """
        result = {}
        
        for feed_type, feed_url in self.feeds.items():
            # Skip if feed URL is blacklisted
            if self.error_tracker and self.error_tracker.should_skip(feed_url):
                self.logger.info(f"Skipping blacklisted RSS feed: {feed_url}")
                continue
                
            self.logger.info(f"Checking {feed_type} feed: {feed_url}")
            
            new_items = self.process_feed(feed_url, feed_type)
            if new_items:
                result[feed_type] = new_items
                self.logger.info(f"Found {len(new_items)} new {feed_type}")
            else:
                self.logger.info(f"No new {feed_type} found")
                
        # Update last check time for all feeds
        now = datetime.now().isoformat()
        for feed_type in self.feeds:
            if feed_type in self.state:
                self.state[feed_type]["last_check"] = now
                
        # Save updated state
        self.save_state()
        
        return result
        
    def process_feed(self, feed_url, feed_type):
        """
        Process a single feed.
        
        Args:
            feed_url: URL of the feed
            feed_type: Type of feed ('movies', 'shows', 'episodes')
            
        Returns:
            List of new items with IDs extracted
        """
        try:
            # Use feedparser with timeout to prevent hanging
            start_time = time.time()
            feed = feedparser.parse(feed_url)  # The feedparser.parse() function doesn't accept timeout
            parse_time = time.time() - start_time
            
            self.logger.debug(f"Feed parsed in {parse_time:.2f}s")
            
            if hasattr(feed, 'bozo_exception'):
                self.logger.warning(f"Feed parsing error: {feed.bozo_exception}")
                
                # Record error in tracker if available
                if self.error_tracker:
                    self.error_tracker.record_error(feed_url, "rss_parse_error")
            
            if not feed.entries:
                self.logger.warning(f"No entries in feed: {feed_url}")
                return []
                
            # Get last seen GUID
            last_guid = None
            if feed_type in self.state:
                last_guid = self.state[feed_type].get("last_guid")
                
            # Get new items
            new_items = self.get_new_items(feed.entries, last_guid)
            
            # Update state with most recent GUID if new items found
            if new_items and feed_type in self.state:
                self.state[feed_type]["last_guid"] = new_items[0].get("guid", "")
                
            # Extract content IDs
            result = []
            for item in new_items:
                content_id = self.extract_id_from_item(item)
                if content_id:
                    # Skip if content ID is blacklisted
                    if self.api_client and hasattr(self.api_client, 'failed_ids') and \
                       content_id in self.api_client.failed_ids:
                        self.logger.info(f"Skipping known failing ID: {content_id}")
                        continue
                        
                    result.append({
                        "id": content_id,
                        "title": item.get("title", ""),
                        "link": item.get("link", ""),
                        "published": item.get("published", ""),
                        "guid": item.get("guid", ""),
                        "source": "rss",
                        "type": feed_type
                    })
                    
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing feed {feed_url}: {e}")
            
            # Record error in tracker if available
            if self.error_tracker:
                self.error_tracker.record_error(feed_url, "rss_process_error")
                
            return []
            
    def get_new_items(self, entries, last_guid):
        """
        Get new items since last seen GUID.
        
        Args:
            entries: Feed entries
            last_guid: Last seen GUID
            
        Returns:
            List of new entries
        """
        if not last_guid:
            # If no last GUID, consider all entries as new
            return entries
            
        new_items = []
        for entry in entries:
            entry_guid = entry.get("guid", entry.get("id", ""))
            
            if entry_guid == last_guid:
                # Found last seen item, stop processing
                break
                
            new_items.append(entry)
            
        return new_items
        
    def extract_id_from_item(self, item):
        """
        Extract content ID from feed item.
        
        Args:
            item: Feed item
            
        Returns:
            Content ID or None
        """
        # Try GUID first (usually contains post ID)
        guid = item.get("guid", item.get("id", ""))
        content_id = self.extract_id_from_guid(guid)
        if content_id:
            return content_id
            
        # Try link
        link = item.get("link", "")
        content_id = self.extract_id_from_link(link)
        if content_id:
            return content_id
            
        return None
        
    def extract_id_from_guid(self, guid):
        """
        Extract content ID from GUID.
        
        Args:
            guid: Item GUID
            
        Returns:
            Content ID or None
        """
        if not guid:
            return None
            
        # WordPress GUIDs often have format like:
        # https://farsiland.com/?p=12345
        try:
            match = re.search(r"[?&]p=(\d+)", guid)
            if match:
                return int(match.group(1))
        except (ValueError, TypeError):
            pass
            
        return None
        
    def extract_id_from_link(self, link):
        """
        Extract content ID from link.
        
        Args:
            link: Item link
            
        Returns:
            Content ID or None
        """
        if not link:
            return None
            
        # Use API client's extraction method if available
        if self.api_client:
            return self.api_client.extract_id_from_url(link)
            
        # Fall back to basic extraction
        try:
            # Pattern like /post-name-12345/ or /12345/
            match = re.search(r"/(\d+)/?$", link)
            if match:
                return int(match.group(1))
                
            # Pattern like /post-name-12345-another-word/
            match = re.search(r"-(\d+)-", link)
            if match:
                return int(match.group(1))
        except (ValueError, TypeError):
            pass
            
        return None
        
    def get_stats(self):
        """
        Get statistics about monitoring state.
        
        Returns:
            Dictionary with monitoring statistics
        """
        stats = {
            "feeds": {},
            "total_feeds": len(self.feeds),
            "active_feeds": 0,
            "last_update": datetime.now().isoformat()
        }
        
        for feed_type, feed_data in self.state.items():
            feed_url = self.feeds.get(feed_type, "")
            stats["feeds"][feed_type] = {
                "url": feed_url,
                "last_check": feed_data.get("last_check"),
                "has_guid": feed_data.get("last_guid") is not None,
                "blacklisted": self.error_tracker and feed_url and self.error_tracker.should_skip(feed_url)
            }
            
            # Count active feeds
            if feed_url and not (self.error_tracker and self.error_tracker.should_skip(feed_url)):
                stats["active_feeds"] += 1
                
        return stats