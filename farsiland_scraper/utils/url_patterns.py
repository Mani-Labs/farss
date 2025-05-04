# File: farsiland_scraper/utils/url_patterns.py
# Version: 1.0.0
# Last Updated: 2025-06-01

"""
URL pattern detection and normalization.

Provides consistent URL handling, especially for non-ASCII URLs,
and flexible pattern matching for content type detection.
"""

import re
import urllib.parse
import unicodedata
from typing import Dict, List, Optional, Tuple, Union

from farsiland_scraper.config import BASE_URL

# Flexible URL patterns for detecting content types
# Using regex patterns with named capture groups for better extraction
URL_PATTERNS = {
    'movies': [
        # Standard pattern
        r"https?://[^/]+/movies/(?P<slug>[^/]+)/?$",
        # Year-specific pattern
        r"https?://[^/]+/movies-\d{4}/(?P<slug>[^/]+)/?$",
        # Old movies pattern
        r"https?://[^/]+/old-iranian-movies/(?P<slug>[^/]+)/?$",
        # General movie pattern (fallback)
        r"https?://[^/]+/(?P<slug>[^/]+)/?\?post_type=movies"
    ],
    'shows': [
        # Standard pattern
        r"https?://[^/]+/tvshows/(?P<slug>[^/]+)/?$",
        # Series-specific pattern
        r"https?://[^/]+/series-\d+/(?P<slug>[^/]+)/?$",
        # Iranian series pattern
        r"https?://[^/]+/iranian-series/(?P<slug>[^/]+)/?$",
        # General show pattern (fallback)
        r"https?://[^/]+/(?P<slug>[^/]+)/?\?post_type=tvshows"
    ],
    'episodes': [
        # Standard pattern
        r"https?://[^/]+/episodes/(?P<slug>[^/]+)/?$",
        # Episode with parameters
        r"https?://[^/]+/episodes/(?P<slug>[^/]+)/?\?",
        # General episode pattern (fallback)
        r"https?://[^/]+/(?P<slug>[^/]+)/?\?post_type=episodes"
    ]
}

# Persian/Arabic digit mapping
PERSIAN_DIGITS = {
    '۰': '0', '۱': '1', '۲': '2', '۳': '3', '۴': '4',
    '۵': '5', '۶': '6', '۷': '7', '۸': '8', '۹': '9'
}

def normalize_persian_digits(text: str) -> str:
    """
    Convert Persian/Arabic digits to Latin digits.
    
    Args:
        text: Text containing Persian digits
        
    Returns:
        Text with normalized digits
    """
    if not text:
        return ""
        
    # Replace Persian/Arabic digits with Latin equivalents
    for persian, latin in PERSIAN_DIGITS.items():
        text = text.replace(persian, latin)
        
    return text

def normalize_text(text: str) -> str:
    """
    Normalize Unicode text for consistent comparison.
    
    Args:
        text: Text to normalize
        
    Returns:
        Normalized text
    """
    if not text:
        return ""
        
    # Convert to NFKC form
    text = unicodedata.normalize('NFKC', text)
    
    # Convert Persian digits
    text = normalize_persian_digits(text)
    
    # Lowercase for case-insensitive comparison
    return text.lower()

def normalize_url(url: str) -> str:
    """
    Normalize URL with proper handling of non-ASCII characters.
    
    Args:
        url: URL to normalize
        
    Returns:
        Normalized URL
    """
    if not url:
        return ""
        
    # Ensure URL has a scheme
    if not url.startswith(('http://', 'https://')):
        url = urllib.parse.urljoin(BASE_URL, url)
        
    # Parse URL to components
    parsed = urllib.parse.urlparse(url)
    
    # Normalize path - ensure proper encoding of non-ASCII characters
    path = urllib.parse.unquote(parsed.path)
    path = normalize_text(path)
    
    # Reconstruct URL with proper encoding
    normalized = urllib.parse.urlunparse((
        parsed.scheme,
        parsed.netloc,
        path.rstrip('/'),  # Remove trailing slash
        parsed.params,
        parsed.query,
        parsed.fragment
    ))
    
    return normalized

def extract_slug_from_url(url: str) -> Optional[str]:
    """
    Extract slug from URL.
    
    Args:
        url: URL to extract from
        
    Returns:
        Slug or None if not found
    """
    normalized = normalize_url(url)
    
    # Try all content types
    for patterns in URL_PATTERNS.values():
        for pattern in patterns:
            match = re.match(pattern, normalized)
            if match and 'slug' in match.groupdict():
                return match.group('slug')
    
    # Fallback: extract last path component
    path = urllib.parse.urlparse(normalized).path.strip('/')
    if path:
        return path.split('/')[-1]
        
    return None

def detect_content_type(url: str) -> Optional[str]:
    """
    Detect content type from URL pattern.
    
    Args:
        url: URL to analyze
        
    Returns:
        Content type ('movies', 'shows', 'episodes') or None
    """
    if not url:
        return None
        
    normalized = normalize_url(url)
    
    # Check against each content type's patterns
    for content_type, patterns in URL_PATTERNS.items():
        for pattern in patterns:
            if re.match(pattern, normalized):
                return content_type
                
    # URL path-based detection as fallback
    path_parts = urllib.parse.urlparse(normalized).path.lower().split('/')
    
    if 'movies' in path_parts:
        return 'movies'
    elif 'tvshows' in path_parts or 'series' in path_parts:
        return 'shows'
    elif 'episodes' in path_parts:
        return 'episodes'
        
    return None

def build_url(content_type: str, slug: str) -> str:
    """
    Build a URL for a specific content type and slug.
    
    Args:
        content_type: Type of content
        slug: Content slug
        
    Returns:
        Full URL
    """
    # Ensure slug is properly encoded
    encoded_slug = urllib.parse.quote(slug)
    
    if content_type == 'movies':
        return f"{BASE_URL}/movies/{encoded_slug}"
    elif content_type == 'shows':
        return f"{BASE_URL}/tvshows/{encoded_slug}"
    elif content_type == 'episodes':
        return f"{BASE_URL}/episodes/{encoded_slug}"
        
    return f"{BASE_URL}/{encoded_slug}"

def is_valid_content_url(url: str, content_type: str) -> bool:
    """
    Check if URL is valid for a specific content type.
    
    Args:
        url: URL to check
        content_type: Expected content type
        
    Returns:
        True if URL is valid for the content type
    """
    detected = detect_content_type(url)
    return detected == content_type