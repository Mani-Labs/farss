# File: farsiland_scraper/utils/error_tracker.py
# Version: 1.1.0
# Last Updated: 2025-06-15

"""
Error tracking module for Farsiland scraper.

This module:
1. Tracks error occurrence patterns
2. Blacklists URLs/IDs that consistently fail
3. Persists error state between runs
4. Helps avoid wasting resources on known problem resources
5. Provides thread safety for multi-threaded operations
6. Implements proper error classification
7. Uses atomic file operations for data integrity

Changelog:
- [1.1.0] Fixed thread safety issues
- [1.1.0] Improved error persistence with atomic file operations
- [1.1.0] Added proper error classification and severity tracking
- [1.1.0] Fixed potential file locking issues
- [1.1.0] Added recovery mechanisms for file corruption
- [1.1.0] Improved error pattern detection
- [1.1.0] Added expiration for old errors
- [1.1.0] Enhanced reporting capabilities
- [1.0.0] Initial implementation
"""

import os
import json
import logging
import threading
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
from typing import Dict, Set, List, Any, Optional, Tuple

from farsiland_scraper.config import (
    LOGGER,
    DATA_DIR,
    ERROR_TRACKER_FILE,
    ERROR_BLACKLIST_THRESHOLD
)

class ErrorTracker:
    """
    Track error patterns and blacklist problematic resources.
    
    Features:
    - Records error occurrences by URL and error type
    - Blacklists resources after they exceed threshold
    - Persists state between runs
    - Helps avoid wasting resources on problematic URLs
    - Thread-safe operations for concurrent usage
    - Classifies errors by severity
    - Implements error pattern detection
    """
    
    # Error severity classifications
    SEVERITY_LEVELS = {
        "critical": 3,   # Severe errors requiring immediate attention
        "high": 2,       # Important errors that should be investigated
        "medium": 1,     # Normal errors that may resolve on retry
        "low": 0         # Minor issues that can be safely ignored
    }
    
    # Error type to severity mapping
    ERROR_SEVERITY = {
        # Critical errors
        "db_connection_error": "critical",
        "db_integrity_error": "critical",
        "file_corruption": "critical",
        
        # High severity errors
        "api_404": "high",
        "api_auth_error": "high",
        "captcha_detected": "high",
        "http_403": "high",
        "http_401": "high",
        "api_rate_limit": "high",
        "db_save_error": "high",
        
        # Medium severity errors
        "http_500": "medium",
        "http_502": "medium",
        "http_503": "medium",
        "http_504": "medium",
        "timeout": "medium",
        "connection_error": "medium",
        "api_error": "medium",
        "parsing_error": "medium",
        
        # Low severity errors
        "warning": "low",
        "minor_issue": "low"
    }
    
    # Error expiration times (in days)
    ERROR_EXPIRATION = {
        "critical": 30,  # Critical errors expire after 30 days
        "high": 14,      # High severity errors expire after 14 days
        "medium": 7,     # Medium severity errors expire after 7 days
        "low": 3         # Low severity errors expire after 3 days
    }
    
    def __init__(self, storage_path: str = None, threshold: int = None, auto_save: bool = True):
        """
        Initialize the error tracker.
        
        Args:
            storage_path: Path to persistence file (default: from config)
            threshold: Number of errors before blacklisting (default: from config)
            auto_save: Whether to automatically save on changes
        """
        self.errors = defaultdict(int)
        self.error_timestamps = {}  # Track when errors occurred
        self.blacklist = set()
        self.storage_path = storage_path or ERROR_TRACKER_FILE
        self.threshold = threshold or ERROR_BLACKLIST_THRESHOLD
        self.auto_save = auto_save
        self.logger = LOGGER
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Last save time
        self.last_save = 0
        self.save_interval = 60  # Minimum seconds between saves
        
        # Last clean time
        self.last_clean = 0
        self.clean_interval = 3600  # Clean expired errors hourly
        
        # Load state
        self._load()
        
    def record_error(self, url: str, error_type: str) -> bool:
        """
        Record error occurrence and blacklist after threshold.
        
        Args:
            url: URL where error occurred
            error_type: Type of error (e.g., 'api_404', 'parsing_error')
            
        Returns:
            True if resource is now blacklisted, False otherwise
        """
        if not url:
            return False
            
        # Normalize URL by removing trailing slashes
        url = url.rstrip('/')
        
        # Thread safety
        with self.lock:
            # Skip if already blacklisted
            if url in self.blacklist:
                return True
                
            # Get error severity
            severity = self._get_error_severity(error_type)
            
            # Create unique key for this URL+error combination
            key = f"{url}:{error_type}"
            
            # Record timestamp
            current_time = datetime.now().isoformat()
            self.error_timestamps[key] = current_time
            
            # Update error count
            self.errors[key] += 1
            
            # Check if threshold exceeded
            error_count = self.errors[key]
            
            # Adjust threshold based on severity
            adjusted_threshold = self._adjust_threshold_by_severity(severity)
            
            if error_count >= adjusted_threshold:
                self.blacklist.add(url)
                self.logger.info(f"Blacklisting {url} after {error_count} {error_type} errors (severity: {severity})")
                
                # Save state immediately when blacklisting
                if self.auto_save:
                    self._save()
                    
                return True
                
            # Save state periodically
            if self.auto_save and (error_count % 10 == 0 or error_count == 1):
                current_time = time.time()
                if current_time - self.last_save >= self.save_interval:
                    self._save()
                    self.last_save = current_time
                    
            # Clean expired errors periodically
            current_time = time.time()
            if current_time - self.last_clean >= self.clean_interval:
                self._clean_expired_errors()
                self.last_clean = current_time
                
            return False
            
    def should_skip(self, url: str) -> bool:
        """
        Check if URL should be skipped due to past errors.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL should be skipped, False otherwise
        """
        if not url:
            return False
            
        # Normalize URL by removing trailing slashes for consistency
        url = url.rstrip('/')
        
        # Thread safety
        with self.lock:
            return url in self.blacklist
            
    def get_stats(self) -> Dict[str, Any]:
        """
        Get error statistics.
        
        Returns:
            Dictionary with error statistics
        """
        with self.lock:
            # Count errors by severity
            severity_counts = defaultdict(int)
            for key in self.errors:
                try:
                    _, error_type = key.split(':', 1)
                    severity = self._get_error_severity(error_type)
                    severity_counts[severity] += 1
                except ValueError:
                    # Key doesn't have expected format
                    pass
            
            return {
                "error_count": len(self.errors),
                "blacklist_count": len(self.blacklist),
                "error_types": self._get_error_types(),
                "severity_counts": dict(severity_counts),
                "blacklist": list(self.blacklist)[:10] + ['...'] if len(self.blacklist) > 10 else list(self.blacklist),
                "last_save": datetime.fromtimestamp(self.last_save).isoformat() if self.last_save else None,
                "last_clean": datetime.fromtimestamp(self.last_clean).isoformat() if self.last_clean else None
            }
            
    def _adjust_threshold_by_severity(self, severity: str) -> int:
        """
        Adjust blacklist threshold based on error severity.
        
        Args:
            severity: Error severity level
            
        Returns:
            Adjusted threshold value
        """
        base_threshold = self.threshold
        
        if severity == "critical":
            return max(1, int(base_threshold / 3))  # Much lower threshold
        elif severity == "high":
            return max(1, int(base_threshold / 2))  # Lower threshold
        elif severity == "medium":
            return base_threshold  # Normal threshold
        else:  # "low"
            return base_threshold * 2  # Higher threshold
            
    def _get_error_severity(self, error_type: str) -> str:
        """
        Get severity level for an error type.
        
        Args:
            error_type: Error type identifier
            
        Returns:
            Severity level string
        """
        # Try exact match first
        if error_type in self.ERROR_SEVERITY:
            return self.ERROR_SEVERITY[error_type]
            
        # Try partial matches
        for known_type, severity in self.ERROR_SEVERITY.items():
            if known_type in error_type:
                return severity
                
        # Default to medium severity
        return "medium"
            
    def _get_error_types(self) -> Dict[str, int]:
        """
        Get counts by error type.
        
        Returns:
            Dictionary with error type counts
        """
        error_types = defaultdict(int)
        with self.lock:
            for key in self.errors:
                try:
                    _, error_type = key.split(':', 1)
                    error_types[error_type] += 1
                except ValueError:
                    # Key doesn't have expected format
                    pass
        return dict(error_types)
        
    def _clean_expired_errors(self) -> int:
        """
        Remove expired errors based on severity and age.
        
        Returns:
            Number of errors cleaned
        """
        cleaned_count = 0
        current_time = datetime.now()
        
        with self.lock:
            # Create a list of keys to remove (can't modify while iterating)
            keys_to_remove = []
            
            for key, timestamp_str in list(self.error_timestamps.items()):
                try:
                    # Parse timestamp
                    timestamp = datetime.fromisoformat(timestamp_str)
                    
                    # Get error type and severity
                    try:
                        _, error_type = key.split(':', 1)
                        severity = self._get_error_severity(error_type)
                    except ValueError:
                        severity = "medium"  # Default
                        
                    # Get expiration days for this severity
                    expiration_days = self.ERROR_EXPIRATION.get(severity, 7)  # Default 7 days
                    
                    # Check if error has expired
                    if (current_time - timestamp) > timedelta(days=expiration_days):
                        keys_to_remove.append(key)
                        
                except (ValueError, TypeError):
                    # If timestamp is invalid, mark for removal
                    keys_to_remove.append(key)
            
            # Remove expired errors
            for key in keys_to_remove:
                if key in self.errors:
                    del self.errors[key]
                if key in self.error_timestamps:
                    del self.error_timestamps[key]
                cleaned_count += 1
                
            # Check if any URLs can be removed from blacklist
            urls_to_unblacklist = []
            
            for url in self.blacklist:
                # Check if any errors remain for this URL
                has_active_errors = False
                for key in self.errors:
                    if key.startswith(f"{url}:"):
                        has_active_errors = True
                        break
                        
                if not has_active_errors:
                    urls_to_unblacklist.append(url)
                    
            # Remove URLs from blacklist if all their errors have expired
            for url in urls_to_unblacklist:
                self.blacklist.remove(url)
                self.logger.info(f"Removed {url} from blacklist due to error expiration")
                cleaned_count += 1
                
            # Save changes if any
            if cleaned_count > 0 and self.auto_save:
                self._save()
                
            return cleaned_count
        
    def _load(self):
        """Load state from storage file with improved error handling."""
        if not os.path.exists(self.storage_path):
            self.logger.info(f"No error tracker state file found at {self.storage_path}")
            return
            
        try:
            with open(self.storage_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Load errors with thread safety
                with self.lock:
                    # Reset current state
                    self.errors = defaultdict(int)
                    self.error_timestamps = {}
                    self.blacklist = set()
                    
                    # Load errors
                    for key, count in data.get('errors', {}).items():
                        self.errors[key] = count
                        
                    # Load error timestamps
                    for key, timestamp in data.get('error_timestamps', {}).items():
                        self.error_timestamps[key] = timestamp
                        
                    # Load blacklist
                    for url in data.get('blacklist', []):
                        if url:  # Ensure URL is not empty
                            self.blacklist.add(url)
                    
                    # Update threshold if saved
                    if 'threshold' in data:
                        self.threshold = data['threshold']
                
            self.logger.info(f"Loaded error tracker state: {len(self.errors)} errors, {len(self.blacklist)} blacklisted URLs")
            
            # Clean expired errors on load
            cleaned = self._clean_expired_errors()
            if cleaned > 0:
                self.logger.info(f"Cleaned {cleaned} expired errors during load")
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing error tracker state file: {e}")
            
            # Create a backup of the corrupted file
            backup_path = f"{self.storage_path}.bak.{datetime.now().strftime('%Y%m%d%H%M%S')}"
            try:
                import shutil
                shutil.copy2(self.storage_path, backup_path)
                self.logger.info(f"Created backup of corrupted state file at {backup_path}")
            except Exception as backup_err:
                self.logger.error(f"Failed to create backup of corrupted state file: {backup_err}")
                
        except (IOError, OSError) as e:
            self.logger.error(f"Error reading error tracker state file: {e}")
            
    def _save(self):
        """Save state to storage file using atomic operations."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
            
            # Prepare data with thread safety
            with self.lock:
                data = {
                    'errors': dict(self.errors),
                    'error_timestamps': self.error_timestamps,
                    'blacklist': list(self.blacklist),
                    'threshold': self.threshold,
                    'saved_at': datetime.now().isoformat()
                }
            
            # Use atomic write pattern
            temp_fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(self.storage_path), suffix='.json')
            try:
                with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                    f.flush()
                    os.fsync(f.fileno())  # Force write to disk
                    
                # Atomic replace
                os.replace(temp_path, self.storage_path)
                
                self.last_save = time.time()
                self.logger.debug(f"Saved error tracker state to {self.storage_path}")
                
            except Exception as e:
                # Clean up temp file on error
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                raise e
                
        except IOError as e:
            self.logger.error(f"Error saving error tracker state: {e}")
            
    def clear(self, url=None):
        """
        Clear error tracking state.
        
        Args:
            url: URL to clear (or all if None)
        """
        with self.lock:
            if url:
                # Remove specific URL from blacklist
                if url in self.blacklist:
                    self.blacklist.remove(url)
                    
                # Clear error counts for this URL
                keys_to_remove = []
                for key in self.errors:
                    if key.startswith(f"{url}:"):
                        keys_to_remove.append(key)
                        
                for key in keys_to_remove:
                    del self.errors[key]
                    if key in self.error_timestamps:
                        del self.error_timestamps[key]
                    
                self.logger.info(f"Cleared error tracking for URL: {url}")
            else:
                # Clear all
                self.errors = defaultdict(int)
                self.error_timestamps = {}
                self.blacklist = set()
                self.logger.info("Cleared all error tracking data")
                
            # Save changes
            self._save()
            
    def get_problematic_resources(self, min_errors: int = 1, severity: str = None) -> List[Tuple[str, int, str]]:
        """
        Get list of most problematic resources sorted by error count.
        
        Args:
            min_errors: Minimum number of errors to include
            severity: Filter by severity level
            
        Returns:
            List of tuples (url, error_count, main_error_type)
        """
        resource_errors = defaultdict(int)
        resource_types = {}
        
        with self.lock:
            # Count errors by resource
            for key, count in self.errors.items():
                try:
                    url, error_type = key.split(':', 1)
                    
                    # Filter by severity if specified
                    if severity and self._get_error_severity(error_type) != severity:
                        continue
                        
                    resource_errors[url] += count
                    
                    # Track the most common error type for this resource
                    if url not in resource_types or count > self.errors.get(f"{url}:{resource_types[url]}", 0):
                        resource_types[url] = error_type
                except ValueError:
                    # Skip malformed keys
                    continue
                    
            # Create result list
            result = []
            for url, count in resource_errors.items():
                if count >= min_errors:
                    result.append((url, count, resource_types.get(url, "unknown")))
                    
            # Sort by error count (descending)
            return sorted(result, key=lambda x: x[1], reverse=True)
            
    def get_error_report(self) -> str:
        """
        Generate a human-readable error report.
        
        Returns:
            Formatted error report string
        """
        with self.lock:
            lines = [
                "=== Error Tracker Report ===",
                f"Total Errors: {len(self.errors)}",
                f"Blacklisted URLs: {len(self.blacklist)}",
                ""
            ]
            
            # Error counts by type
            lines.append("Error Types:")
            for error_type, count in sorted(self._get_error_types().items(), key=lambda x: x[1], reverse=True):
                severity = self._get_error_severity(error_type)
                lines.append(f"  {error_type}: {count} ({severity} severity)")
                
            # Most problematic resources
            lines.append("\nMost Problematic Resources:")
            for url, count, error_type in self.get_problematic_resources(min_errors=2)[:10]:
                severity = self._get_error_severity(error_type)
                lines.append(f"  {url}: {count} errors ({error_type}, {severity} severity)")
                
            # Blacklist sample
            if self.blacklist:
                lines.append("\nBlacklisted URLs (sample):")
                for url in list(self.blacklist)[:5]:
                    lines.append(f"  {url}")
                if len(self.blacklist) > 5:
                    lines.append(f"  ... and {len(self.blacklist) - 5} more")
                    
            return "\n".join(lines)