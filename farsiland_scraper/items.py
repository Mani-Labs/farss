# File: farsiland_scraper/items.py
# Version: 4.0.0
# Last Updated: 2025-06-01

"""
Item definitions for Farsiland scraper.

Defines structured data models for:
- Shows/Series
- Episodes
- Movies
- Video Files

Features:
- Comprehensive field definitions with type annotations
- Validation methods for data integrity
- Support for Persian/Arabic text handling
- Fields for incremental update tracking
- API integration fields
"""

import scrapy
import re
import unicodedata
from datetime import datetime
from typing import Dict, List, Union, Optional, Any, cast

class BaseItem(scrapy.Item):
    """Base class for all items with common fields and methods."""
    
    # Timestamps and metadata
    last_scraped = scrapy.Field(serializer=str)  # When the item was last scraped
    cached_at = scrapy.Field(serializer=str)  # When the item was cached
    source = scrapy.Field(serializer=str)  # Source of the data (e.g., 'html', 'api')
    
    # API integration fields
    api_id = scrapy.Field(serializer=int)  # WordPress post ID
    api_source = scrapy.Field(serializer=str)  # Source of API data (e.g., "wp-json/v2")
    modified_gmt = scrapy.Field(serializer=str)  # Last modified date from API
    
    # Incremental update tracking
    is_new = scrapy.Field(serializer=bool)  # Whether this is a new or updated item
    is_modified = scrapy.Field(serializer=bool)  # Whether content was modified since last run
    
    # URL tracking
    url = scrapy.Field(serializer=str)  # Primary key, unique URL of the item
    sitemap_url = scrapy.Field(serializer=str)  # URL in sitemap (can be different from url)
    lastmod = scrapy.Field(serializer=str)  # Last modification timestamp from sitemap
    
    def __init__(self, *args, **kwargs):
        """Initialize with timestamps and default values."""
        super().__init__(*args, **kwargs)
        # Set default timestamp if not provided
        if 'last_scraped' not in kwargs:
            self['last_scraped'] = datetime.now().isoformat()
            
        # Set default for new item flag if not provided
        if 'is_new' not in kwargs:
            self['is_new'] = True
            
        # Set default for modified flag if not provided
        if 'is_modified' not in kwargs:
            self['is_modified'] = False
    
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
        Normalize URL to ensure consistent format.
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL
        """
        if not url:
            return ""
            
        # Remove trailing slash
        url = url.rstrip("/")
        
        # Handle non-ASCII characters in URLs
        from urllib.parse import quote, unquote, urlparse, urlunparse
        
        try:
            # Parse URL to components
            parsed = urlparse(url)
            
            # Normalize path - ensure proper encoding of non-ASCII characters
            path = unquote(parsed.path)
            path = self.normalize_text(path)
            
            # Reconstruct URL with proper encoding
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc,
                quote(path),
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
            
            return normalized
        except Exception:
            # If URL parsing fails, return original
            return url
    
    def validate(self) -> List[str]:
        """
        Validate item data.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Check required fields
        if not self.get('url'):
            errors.append("URL is required")
            
        # Validate URL format
        if self.get('url') and not self.get('url').startswith(('http://', 'https://')):
            errors.append("URL must start with http:// or https://")
            
        return errors


class ShowItem(BaseItem):
    """
    Item representing a TV show/series.
    
    A show contains general metadata and can have multiple seasons and episodes.
    Related items:
    - SeasonItem: Represents a season of this show (many-to-one)
    - EpisodeItem: Represents episodes in this show (many-to-one)
    """
    # Basic metadata
    title_en = scrapy.Field(serializer=str)  # English title
    title_fa = scrapy.Field(serializer=str)  # Farsi title
    poster = scrapy.Field(serializer=str)  # URL to poster image
    description = scrapy.Field(serializer=str)  # Show description/synopsis
    first_air_date = scrapy.Field(serializer=str)  # Date of first episode airing
    last_aired = scrapy.Field(serializer=str)  # Date of most recent episode (new)
    
    # Ratings
    rating = scrapy.Field(serializer=float)  # Average rating (0-10)
    rating_count = scrapy.Field(serializer=int)  # Number of ratings
    
    # Structure
    season_count = scrapy.Field(serializer=int)  # Number of seasons
    episode_count = scrapy.Field(serializer=int)  # Total number of episodes
    
    # Categories and people
    genres = scrapy.Field()  # List of genre strings
    directors = scrapy.Field()  # List of director names
    cast = scrapy.Field()  # List of cast member names
    
    # Engagement metrics
    social_shares = scrapy.Field(serializer=int)  # Number of social media shares
    comments_count = scrapy.Field(serializer=int)  # Number of comments
    
    # Season data
    seasons = scrapy.Field()  # List of season data (can contain SeasonItem objects)
    
    # Episode URL tracking (for cross-reference)
    episode_urls = scrapy.Field()  # List of episode URLs discovered in this show
    
    # Language
    language_code = scrapy.Field(serializer=str)  # Language code (e.g., 'fa', 'en')
    
    def validate(self) -> List[str]:
        """
        Validate show data.
        
        Returns:
            List of validation error messages
        """
        errors = super().validate()
        
        # Check required fields
        if not self.get('title_en'):
            errors.append("English title is required")
            
        # Validate numeric fields
        if self.get('rating') is not None and (not isinstance(self['rating'], (int, float)) or 
                                              self['rating'] < 0 or self['rating'] > 10):
            errors.append("Rating must be a number between 0 and 10")
            
        if self.get('rating_count') is not None and (not isinstance(self['rating_count'], int) or 
                                                    self['rating_count'] < 0):
            errors.append("Rating count must be a non-negative integer")
            
        if self.get('season_count') is not None and (not isinstance(self['season_count'], int) or 
                                                    self['season_count'] < 0):
            errors.append("Season count must be a non-negative integer")
            
        if self.get('episode_count') is not None and (not isinstance(self['episode_count'], int) or 
                                                     self['episode_count'] < 0):
            errors.append("Episode count must be a non-negative integer")
            
        # Validate list fields
        for field in ['genres', 'directors', 'cast', 'seasons', 'episode_urls']:
            if self.get(field) is not None and not isinstance(self[field], list):
                errors.append(f"{field} must be a list")
                
        # Normalize Persian text
        if self.get('title_fa'):
            self['title_fa'] = self.normalize_text(self['title_fa'])
            
        return errors


class MovieItem(BaseItem):
    """
    Item representing a movie.
    
    A movie contains metadata about a standalone film.
    """
    # Basic metadata
    title_en = scrapy.Field(serializer=str)  # English title
    title_fa = scrapy.Field(serializer=str)  # Farsi title
    poster = scrapy.Field(serializer=str)  # URL to poster image
    description = scrapy.Field(serializer=str)  # Movie description/synopsis
    release_date = scrapy.Field(serializer=str)  # Release date
    year = scrapy.Field(serializer=int)  # Release year
    
    # Ratings
    rating = scrapy.Field(serializer=float)  # Average rating (0-10)
    rating_count = scrapy.Field(serializer=int)  # Number of ratings
    
    # Categories and people
    genres = scrapy.Field()  # List of genre strings
    directors = scrapy.Field()  # List of director names
    cast = scrapy.Field()  # List of cast member names
    
    # Engagement metrics
    social_shares = scrapy.Field(serializer=int)  # Number of social media shares
    comments_count = scrapy.Field(serializer=int)  # Number of comments
    
    # Video files
    video_files = scrapy.Field()  # List of video files (VideoFileItem objects)
    
    # Language
    language_code = scrapy.Field(serializer=str)  # Language code (e.g., 'fa', 'en')
    
    def validate(self) -> List[str]:
        """
        Validate movie data.
        
        Returns:
            List of validation error messages
        """
        errors = super().validate()
        
        # Check required fields
        if not self.get('title_en'):
            errors.append("English title is required")
            
        # Validate numeric fields
        if self.get('rating') is not None and (not isinstance(self['rating'], (int, float)) or 
                                              self['rating'] < 0 or self['rating'] > 10):
            errors.append("Rating must be a number between 0 and 10")
            
        if self.get('rating_count') is not None and (not isinstance(self['rating_count'], int) or 
                                                    self['rating_count'] < 0):
            errors.append("Rating count must be a non-negative integer")
            
        if self.get('year') is not None:
            try:
                year = int(self['year'])
                if year < 1900 or year > 2100:
                    errors.append("Year must be between 1900 and 2100")
            except (ValueError, TypeError):
                errors.append("Year must be a valid integer")
                
        # Validate list fields
        for field in ['genres', 'directors', 'cast', 'video_files']:
            if self.get(field) is not None and not isinstance(self[field], list):
                errors.append(f"{field} must be a list")
                
        # Normalize Persian text
        if self.get('title_fa'):
            self['title_fa'] = self.normalize_text(self['title_fa'])
            
        return errors


class EpisodeItem(BaseItem):
    """
    Item representing a TV show episode.
    
    An episode belongs to a TV show and contains metadata and video links.
    """
    # Relationship
    show_url = scrapy.Field(serializer=str)  # URL of the parent show
    show_id = scrapy.Field(serializer=int)  # ID of the parent show (for API)
    
    # Episode metadata
    season_number = scrapy.Field(serializer=int)  # Season number
    episode_number = scrapy.Field(serializer=int)  # Episode number within the season
    title = scrapy.Field(serializer=str)  # Episode title
    air_date = scrapy.Field(serializer=str)  # Air date
    thumbnail = scrapy.Field(serializer=str)  # Thumbnail image URL
    
    # Video files
    video_files = scrapy.Field()  # List of video files (VideoFileItem objects)
    
    # Language
    language_code = scrapy.Field(serializer=str)  # Language code (e.g., 'fa', 'en')
    
    def validate(self) -> List[str]:
        """
        Validate episode data.
        
        Returns:
            List of validation error messages
        """
        errors = super().validate()
        
        # Check required fields
        if not self.get('show_url'):
            errors.append("Show URL is required")
            
        # Validate numeric fields
        if self.get('season_number') is not None and (not isinstance(self['season_number'], int) or 
                                                     self['season_number'] < 0):
            errors.append("Season number must be a non-negative integer")
            
        if self.get('episode_number') is not None and (not isinstance(self['episode_number'], int) or 
                                                      self['episode_number'] < 0):
            errors.append("Episode number must be a non-negative integer")
            
        # Validate video files
        if self.get('video_files') is not None and not isinstance(self['video_files'], list):
            errors.append("Video files must be a list")
            
        # Normalize show URL to ensure consistent format
        if self.get('show_url'):
            self['show_url'] = self.normalize_url(self['show_url'])
            
        return errors


class VideoFileItem(scrapy.Item):
    """
    Item representing a video file.
    
    A video file contains information about a playable media file, including
    its quality, URL, and other metadata.
    """
    # Metadata
    quality = scrapy.Field(serializer=str)  # Quality label (e.g., "720", "1080")
    size = scrapy.Field(serializer=str)  # File size (e.g., "1.2 GB")
    
    # URLs
    url = scrapy.Field(serializer=str)  # Primary download URL
    mirror_url = scrapy.Field(serializer=str)  # Mirror/alternate download URL
    
    # Additional metadata
    format = scrapy.Field(serializer=str)  # File format (e.g., "mp4", "mkv")
    resolution = scrapy.Field(serializer=str)  # Full resolution (e.g., "1920x1080")
    bitrate = scrapy.Field(serializer=str)  # Bitrate (e.g., "5 Mbps")
    
    def validate(self) -> List[str]:
        """
        Validate video file data.
        
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Check required fields
        if not self.get('url'):
            errors.append("URL is required")
            
        # Quality validation
        if self.get('quality'):
            # Convert common quality labels to standard format
            quality_map = {
                "sd": "480",
                "hd": "720",
                "fullhd": "1080", 
                "ultrahd": "2160",
                "4k": "2160", 
                "8k": "4320"
            }
            
            quality = self.get('quality').lower()
            if quality in quality_map:
                self['quality'] = quality_map[quality]
                
            # Extract quality from string if it contains numbers
            if not self['quality'].isdigit():
                match = re.search(r'(\d+)', self['quality'])
                if match:
                    self['quality'] = match.group(1)
            
        return errors