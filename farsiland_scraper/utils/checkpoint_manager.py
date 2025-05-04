# File: farsiland_scraper/utils/checkpoint_manager.py
# Version: 1.1.0
# Last Updated: 2025-06-15

"""
Checkpoint manager for resumable crawling.

Allows saving and resuming crawler state to continue interrupted crawls
without restarting from scratch.

Changelog:
- [1.1.0] Fixed atomic file operations to prevent data corruption
- [1.1.0] Improved thread safety for concurrent access
- [1.1.0] Enhanced error recovery mechanisms
- [1.1.0] Fixed cross-platform path handling issues
- [1.1.0] Added file validation to detect corruption
- [1.1.0] Improved checkpoint file organization
- [1.1.0] Added checkpoint verification before usage
- [1.1.0] Enhanced logging and reporting capabilities
"""

import os
import json
import time
import logging
import tempfile
import threading
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Any, Optional, Union

from farsiland_scraper.config import LOGGER, DATA_DIR

class CheckpointManager:
    """
    Manages crawler state checkpoints for resumable operation.
    
    Features:
    - Save crawler state at intervals
    - Track URLs that have been processed
    - Resume crawling from last checkpoint
    - Atomic file operations for reliability
    - Thread-safe operations for concurrent access
    """
    
    def __init__(self, checkpoint_dir: Optional[str] = None, checkpoint_interval: int = 60, item_threshold: int = 10000):
        """
        Initialize checkpoint manager.
        
        Args:
            checkpoint_dir: Directory to store checkpoints
            checkpoint_interval: How often to save checkpoints (seconds)
            item_threshold: Save after this many items have been processed
        """
        self.checkpoint_dir = Path(checkpoint_dir or os.path.join(DATA_DIR, "checkpoints"))
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_interval = max(10, checkpoint_interval)  # Minimum 10 seconds
        self.item_threshold = max(1, item_threshold)  # Minimum 1 item
        self.last_checkpoint_time = 0
        self.processed_count = 0
        self.processed_urls: Dict[str, Set[str]] = {
            "movies": set(),
            "shows": set(),
            "episodes": set()
        }
        self.pending_urls: Dict[str, List[str]] = {
            "movies": [],
            "shows": [],
            "episodes": []
        }
        self.logger = LOGGER
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Track checkpoint history
        self.checkpoint_history = []
        self.max_history = 5  # Maximum number of checkpoints to track
        
        # Try to load latest checkpoint
        self._load_latest_checkpoint()
        
    def _get_checkpoint_path(self, timestamp: Optional[int] = None) -> Path:
        """
        Get path for checkpoint file using pathlib for cross-platform compatibility.
        
        Args:
            timestamp: Optional timestamp for the checkpoint
            
        Returns:
            Path to checkpoint file
        """
        if timestamp is None:
            timestamp = int(time.time())
            
        return self.checkpoint_dir / f"checkpoint_{timestamp}.json"
        
    def _load_latest_checkpoint(self) -> bool:
        """
        Load the most recent checkpoint if available.
        
        Returns:
            True if checkpoint loaded, False otherwise
        """
        try:
            # Find all checkpoint files
            checkpoint_files = []
            
            # Use pathlib for better cross-platform compatibility
            for path in self.checkpoint_dir.glob("checkpoint_*.json"):
                if path.is_file() and path.name.startswith("checkpoint_") and path.suffix == ".json":
                    try:
                        # Extract timestamp from filename
                        timestamp_str = path.stem.split('_')[1]
                        timestamp = int(timestamp_str)
                        checkpoint_files.append((timestamp, path))
                    except (IndexError, ValueError):
                        self.logger.warning(f"Invalid checkpoint filename: {path.name}")
            
            if not checkpoint_files:
                self.logger.info("No checkpoints found")
                return False
                
            # Sort by timestamp (descending)
            checkpoint_files.sort(reverse=True)
            
            # Track checkpoint history
            self.checkpoint_history = [path for _, path in checkpoint_files]
            
            # Use the most recent checkpoint
            _, latest_checkpoint = checkpoint_files[0]
            self.logger.info(f"Loading checkpoint: {latest_checkpoint}")
            
            # Check file integrity
            if not self._verify_checkpoint_file(latest_checkpoint):
                self.logger.error(f"Checkpoint file {latest_checkpoint} is invalid or corrupted")
                # Try next checkpoint if available
                if len(checkpoint_files) > 1:
                    _, backup_checkpoint = checkpoint_files[1]
                    self.logger.info(f"Trying backup checkpoint: {backup_checkpoint}")
                    if self._verify_checkpoint_file(backup_checkpoint):
                        latest_checkpoint = backup_checkpoint
                    else:
                        self.logger.error("Backup checkpoint is also invalid")
                        return False
                else:
                    return False
            
            # Load the verified checkpoint
            with self.lock:
                with open(latest_checkpoint, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Convert lists back to sets for processed URLs
                    for category in self.processed_urls:
                        self.processed_urls[category] = set(data.get("processed_urls", {}).get(category, []))
                        
                    # Load pending URLs
                    for category in self.pending_urls:
                        self.pending_urls[category] = data.get("pending_urls", {}).get(category, [])
                        
                    # Load processed count
                    self.processed_count = data.get("processed_count", 0)
                    
                    # Store loading time as last checkpoint time
                    self.last_checkpoint_time = time.time()
                    
            total_processed = sum(len(urls) for urls in self.processed_urls.values())
            total_pending = sum(len(urls) for urls in self.pending_urls.values())
            
            self.logger.info(f"Loaded checkpoint with {total_processed} processed URLs and {total_pending} pending URLs")
            return True
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing checkpoint file: {e}")
            return False
        except (IOError, OSError) as e:
            self.logger.error(f"Error loading checkpoint: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error loading checkpoint: {e}")
            return False
            
    def _verify_checkpoint_file(self, checkpoint_path: Path) -> bool:
        """
        Verify checkpoint file integrity.
        
        Args:
            checkpoint_path: Path to checkpoint file
            
        Returns:
            True if file is valid, False otherwise
        """
        if not checkpoint_path.exists():
            return False
            
        # Check file size (non-zero)
        if checkpoint_path.stat().st_size == 0:
            self.logger.warning(f"Empty checkpoint file: {checkpoint_path}")
            return False
            
        # Try to parse JSON
        try:
            with open(checkpoint_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Check for required fields
            if not all(key in data for key in ["processed_urls", "pending_urls", "processed_count"]):
                self.logger.warning(f"Checkpoint file missing required fields: {checkpoint_path}")
                return False
                
            return True
        except json.JSONDecodeError:
            self.logger.warning(f"Invalid JSON in checkpoint file: {checkpoint_path}")
            return False
        except Exception as e:
            self.logger.warning(f"Error verifying checkpoint file {checkpoint_path}: {e}")
            return False
            
    def save_checkpoint(self, force: bool = False) -> bool:
        """
        Save current state to checkpoint file.
        
        Args:
            force: Force save even if interval hasn't elapsed
            
        Returns:
            True if checkpoint saved, False otherwise
        """
        current_time = time.time()
        
        # Check if enough time has passed since last checkpoint or processed enough items
        time_threshold_met = (current_time - self.last_checkpoint_time >= self.checkpoint_interval)
        count_threshold_met = (self.processed_count % self.item_threshold == 0 and self.processed_count > 0)
        
        if not force and not time_threshold_met and not count_threshold_met:
            return False
            
        try:
            # Ensure directory exists
            self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate checkpoint filename with timestamp
            timestamp = int(current_time)
            checkpoint_path = self._get_checkpoint_path(timestamp)
            
            # Prepare data for serialization (thread-safe)
            with self.lock:
                checkpoint_data = {
                    "timestamp": current_time,
                    "processed_count": self.processed_count,
                    "processed_urls": {
                        category: list(urls) for category, urls in self.processed_urls.items()
                    },
                    "pending_urls": self.pending_urls,
                    "metadata": {
                        "created_at": datetime.now().isoformat(),
                        "processed_count": sum(len(urls) for urls in self.processed_urls.values()),
                        "pending_count": sum(len(urls) for urls in self.pending_urls.values())
                    }
                }
            
            # Use atomic write pattern with temp file
            # Create temp file in same directory as target for atomic move
            temp_fd, temp_path = tempfile.mkstemp(dir=str(self.checkpoint_dir), suffix='.json')
            try:
                with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                    json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
                    # Ensure data is written to disk
                    f.flush()
                    os.fsync(f.fileno())
                    
                # Perform atomic replace
                os.replace(temp_path, str(checkpoint_path))
                
                self.last_checkpoint_time = current_time
                self.logger.info(f"Saved checkpoint: {checkpoint_path}")
                
                # Add to history and maintain max history size
                self.checkpoint_history.insert(0, checkpoint_path)
                if len(self.checkpoint_history) > self.max_history:
                    self._cleanup_old_checkpoints(keep=self.max_history)
                
                return True
            
            except Exception as e:
                self.logger.error(f"Error writing checkpoint: {e}")
                # Clean up temp file if it exists
                if os.path.exists(temp_path):
                    try:
                        os.unlink(temp_path)
                    except:
                        pass
                return False
                
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {e}")
            return False
            
    def _cleanup_old_checkpoints(self, keep: int = 5) -> None:
        """
        Remove old checkpoints, keeping only the most recent ones.
        
        Args:
            keep: Number of most recent checkpoints to keep
        """
        try:
            # Update checkpoint history
            checkpoint_files = []
            for path in self.checkpoint_dir.glob("checkpoint_*.json"):
                if path.is_file() and path.name.startswith("checkpoint_") and path.suffix == ".json":
                    try:
                        # Extract timestamp from filename
                        timestamp_str = path.stem.split('_')[1]
                        timestamp = int(timestamp_str)
                        checkpoint_files.append((timestamp, path))
                    except (IndexError, ValueError):
                        continue
            
            # Sort by timestamp (descending)
            checkpoint_files.sort(reverse=True)
            
            # Keep the most recent 'keep' files
            to_keep = {str(path) for _, path in checkpoint_files[:keep]}
            for _, path in checkpoint_files[keep:]:
                try:
                    if str(path) not in to_keep:
                        path.unlink()  # Use pathlib's unlink method
                except Exception as e:
                    self.logger.warning(f"Error removing old checkpoint {path}: {e}")
                    
            # Update history
            self.checkpoint_history = [path for _, path in checkpoint_files[:keep]]
                    
        except Exception as e:
            self.logger.error(f"Error cleaning up old checkpoints: {e}")
            
    def mark_as_processed(self, url: str, content_type: str) -> None:
        """
        Mark URL as processed.
        
        Args:
            url: The URL that was processed
            content_type: Type of content (movies, shows, episodes)
        """
        if not url:
            return
            
        # Clean URL
        url = url.strip().rstrip('/')
        
        with self.lock:
            if content_type in self.processed_urls:
                self.processed_urls[content_type].add(url)
                self.processed_count += 1
                
                # Try to save checkpoint if we've hit the item threshold
                if self.processed_count % self.item_threshold == 0:
                    self.save_checkpoint()
                    
                # Remove from pending if present
                if content_type in self.pending_urls and url in self.pending_urls[content_type]:
                    self.pending_urls[content_type].remove(url)
                
    def mark_all_as_processed(self, urls: List[str], content_type: str) -> None:
        """
        Mark multiple URLs as processed at once.
        
        Args:
            urls: List of URLs that were processed
            content_type: Type of content (movies, shows, episodes)
        """
        if not urls:
            return
            
        with self.lock:
            if content_type in self.processed_urls:
                # Clean URLs
                cleaned_urls = {url.strip().rstrip('/') for url in urls if url}
                
                # Add to processed
                self.processed_urls[content_type].update(cleaned_urls)
                self.processed_count += len(cleaned_urls)
                
                # Remove from pending
                if content_type in self.pending_urls:
                    self.pending_urls[content_type] = [
                        url for url in self.pending_urls[content_type] 
                        if url.strip().rstrip('/') not in cleaned_urls
                    ]
                
                # Try to save checkpoint if we've hit the item threshold
                if self.processed_count % self.item_threshold == 0:
                    self.save_checkpoint()
                
    def is_processed(self, url: str, content_type: str) -> bool:
        """
        Check if URL has already been processed.
        
        Args:
            url: URL to check
            content_type: Type of content
            
        Returns:
            True if URL has been processed
        """
        if not url:
            return False
            
        # Clean URL
        url = url.strip().rstrip('/')
        
        with self.lock:
            return url in self.processed_urls.get(content_type, set())
        
    def add_pending(self, url: str, content_type: str) -> None:
        """
        Add URL to pending list.
        
        Args:
            url: URL to add
            content_type: Type of content
        """
        if not url:
            return
            
        # Clean URL
        url = url.strip().rstrip('/')
        
        with self.lock:
            if content_type in self.pending_urls and url not in self.processed_urls.get(content_type, set()):
                if url not in self.pending_urls[content_type]:
                    self.pending_urls[content_type].append(url)
                
    def add_all_pending(self, urls: List[str], content_type: str) -> None:
        """
        Add multiple URLs to pending list at once.
        
        Args:
            urls: List of URLs to add
            content_type: Type of content
        """
        if not urls:
            return
            
        with self.lock:
            if content_type in self.pending_urls:
                processed_set = self.processed_urls.get(content_type, set())
                
                # Clean URLs and filter already processed ones
                for url in urls:
                    if not url:
                        continue
                        
                    clean_url = url.strip().rstrip('/')
                    if clean_url not in processed_set and clean_url not in self.pending_urls[content_type]:
                        self.pending_urls[content_type].append(clean_url)
                
    def get_pending_urls(self, content_type: str, limit: int = 0) -> List[str]:
        """
        Get pending URLs for a content type.
        
        Args:
            content_type: Type of content
            limit: Maximum number of URLs to return (0 for all)
            
        Returns:
            List of pending URLs
        """
        with self.lock:
            urls = self.pending_urls.get(content_type, [])
            if limit > 0:
                return urls[:limit]
            return urls.copy()  # Return a copy to avoid modification issues
        
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about checkpoint state.
        
        Returns:
            Dictionary with checkpoint statistics
        """
        with self.lock:
            stats = {
                "processed_counts": {
                    category: len(urls) for category, urls in self.processed_urls.items()
                },
                "pending_counts": {
                    category: len(urls) for category, urls in self.pending_urls.items()
                },
                "total_processed": self.processed_count,
                "last_checkpoint": self.last_checkpoint_time,
                "checkpoint_age": time.time() - self.last_checkpoint_time if self.last_checkpoint_time > 0 else None,
                "checkpoints_available": len(self.checkpoint_history),
                "checkpoint_dir": str(self.checkpoint_dir)
            }
            return stats
            
    def export_state(self, output_path: str) -> bool:
        """
        Export current state to a specific file.
        
        Args:
            output_path: Path to export file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Prepare data for serialization (thread-safe)
            with self.lock:
                state_data = {
                    "export_time": datetime.now().isoformat(),
                    "processed_count": self.processed_count,
                    "processed_urls": {
                        category: list(urls) for category, urls in self.processed_urls.items()
                    },
                    "pending_urls": self.pending_urls,
                    "metadata": {
                        "processed_count": sum(len(urls) for urls in self.processed_urls.values()),
                        "pending_count": sum(len(urls) for urls in self.pending_urls.values())
                    }
                }
            
            # Use atomic write pattern
            temp_fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(output_path), suffix='.json')
            try:
                with os.fdopen(temp_fd, 'w', encoding='utf-8') as f:
                    json.dump(state_data, f, indent=2, ensure_ascii=False)
                    f.flush()
                    os.fsync(f.fileno())
                    
                # Atomic replace
                os.replace(temp_path, output_path)
                self.logger.info(f"Exported state to {output_path}")
                return True
                
            except Exception as e:
                self.logger.error(f"Error writing export file: {e}")
                # Clean up temp file if it exists
                if os.path.exists(temp_path):
                    try:
                        os.unlink(temp_path)
                    except:
                        pass
                return False
                
        except Exception as e:
            self.logger.error(f"Error exporting state: {e}")
            return False
            
    def import_state(self, input_path: str) -> bool:
        """
        Import state from a file.
        
        Args:
            input_path: Path to import file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not os.path.exists(input_path):
                self.logger.error(f"Import file not found: {input_path}")
                return False
                
            with open(input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            with self.lock:
                # Convert lists back to sets for processed URLs
                for category in self.processed_urls:
                    self.processed_urls[category] = set(data.get("processed_urls", {}).get(category, []))
                    
                # Load pending URLs
                for category in self.pending_urls:
                    self.pending_urls[category] = data.get("pending_urls", {}).get(category, [])
                    
                # Load processed count
                self.processed_count = data.get("processed_count", 0)
                
            # Create a checkpoint to save the imported state
            self.save_checkpoint(force=True)
            
            total_processed = sum(len(urls) for urls in self.processed_urls.values())
            total_pending = sum(len(urls) for urls in self.pending_urls.values())
            
            self.logger.info(f"Imported state with {total_processed} processed URLs and {total_pending} pending URLs")
            return True
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing import file: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error importing state: {e}")
            return False
            
    def clear_state(self) -> bool:
        """
        Clear all state.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self.lock:
                self.processed_urls = {
                    "movies": set(),
                    "shows": set(),
                    "episodes": set()
                }
                self.pending_urls = {
                    "movies": [],
                    "shows": [],
                    "episodes": []
                }
                self.processed_count = 0
                
            # Save empty state
            result = self.save_checkpoint(force=True)
            if result:
                self.logger.info("Cleared checkpoint state")
            return result
            
        except Exception as e:
            self.logger.error(f"Error clearing state: {e}")
            return False