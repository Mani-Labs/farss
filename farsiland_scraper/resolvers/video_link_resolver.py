# File: farsiland_scraper/resolvers/video_link_resolver.py
# Version: 4.1.0
# Last Updated: 2025-05-25

"""
Video link resolver for Farsiland scraper.

Handles extracting MP4 links from form submissions, with support for:
- Automatic handling of redirects
- Retrying on transient failures
- Quality extraction from URLs
- Consistent error handling
- Download size limits

Changelog:
- [4.1.0] Fixed Unicode arrow encoding issue in logs
- [4.1.0] Added size limit for video downloads
- [4.1.0] Improved quality detection for flnd.buzz URLs
- [4.0.0] Complete rewrite with improved architecture
- [4.0.0] Simplified redirect handling logic
- [4.0.0] Added configurable retry mechanism
- [4.0.0] Improved quality extraction patterns and reliability
- [4.0.0] Implemented consistent error handling
- [4.0.0] Added detailed logging for troubleshooting
- [3.3.0] Fixed redirect handling for MP4 extraction
"""

import re
import time
import asyncio
import aiohttp
import logging
from typing import List, Dict, Optional, Tuple, Any
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from farsiland_scraper.config import (
    LOGGER, 
    BASE_URL, 
    REQUEST_RETRY_COUNT, 
    REQUEST_RETRY_DELAY,
    MAX_VIDEO_SIZE
)

# Quality patterns for URL matching, ordered by specificity
QUALITY_PATTERNS = [
    # Direct quality indicators
    r"[/-](\d{3,4})[pP][/-]",            # Format: -720p- or /1080p/
    r"\.(\d{3,4})[pP]\.",                # Format: .720p.
    r"quality[=/-](\d{3,4})",            # Format: quality=1080 or quality-720
    r"resolution[_\-](\d{3,4})",         # Format: resolution_720 or resolution-1080
    # Position-based patterns
    r"/(\d{3,4})[pP]?/",                 # Format: /720/ or /1080p/
    r"[_\-](\d{3,4})[pP]?[_\-]",         # Format: _720_ or -1080p-
    r"[-_.](\d{3,4})[-_.]",              # Format: -720. or _1080-
    # Fallback patterns for less structured URLs
    r"[/_-](\d{3,4})[/_-]",              # Format: /720/ or _1080_
    r"[^0-9](\d{3,4})[^0-9]"             # Any 3-4 digit number surrounded by non-digits
]

# Text phrases that indicate quality
QUALITY_PHRASES = {
    "low": "480",
    "medium": "720", 
    "high": "1080",
    "hd": "720", 
    "fullhd": "1080", 
    "ultrahd": "2160",
    "4k": "2160", 
    "8k": "4320"
}

# Valid quality values to ensure realistic results
VALID_QUALITIES = ["360", "480", "720", "1080", "1440", "2160", "4320"]

# Common MP4 container selector patterns
MP4_SELECTORS = [
    "div.inside a[href$='.mp4']",        # Inside wrapper with mp4 extension
    "a.btn[href$='.mp4']",               # Button with mp4 extension
    "a.download-btn[href$='.mp4']",      # Download button with mp4 extension
    "a.video-link[href$='.mp4']",        # Video link with mp4 extension
    "a[href$='.mp4']"                    # Any link with mp4 extension (fallback)
]

class VideoLinkResolver:
    """
    Class to handle resolving video links from the website.
    
    Features:
    - Handles form submissions
    - Follows redirects to find MP4 links
    - Retries on transient failures
    - Extracts quality information
    - Limits download sizes
    """
    
    def __init__(self, 
                 base_url: str = BASE_URL, 
                 max_retries: int = REQUEST_RETRY_COUNT,
                 retry_delay: int = REQUEST_RETRY_DELAY):
        """
        Initialize the video link resolver.
        
        Args:
            base_url: Base URL of the site
            max_retries: Maximum number of retry attempts
            retry_delay: Base delay between retries in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.post_url = f"{self.base_url}/get/"
        
    async def get_video_links(self, session: aiohttp.ClientSession, fileid: str) -> List[Dict[str, str]]:
        """
        Get video links from a form submission using the fileid.
        
        Args:
            session: Aiohttp client session
            fileid: File ID to retrieve
            
        Returns:
            List of video file dictionaries with quality, url, and optional mirror_url
        """
        if not fileid:
            LOGGER.warning("Empty fileid provided to get_video_links")
            return []
            
        try:
            # Ensure cookies are established
            await session.get(self.base_url)
            
            # Try with configurable retries
            for attempt in range(self.max_retries + 1):  # +1 for the initial attempt
                try:
                    LOGGER.info(f"Sending POST request to {self.post_url} with fileid={fileid} (attempt {attempt+1}/{self.max_retries+1})")
                    video_links = await self._submit_form_and_get_links(session, fileid)
                    if video_links:
                        return video_links
                        
                except aiohttp.ClientError as e:
                    if attempt < self.max_retries:
                        backoff_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                        LOGGER.warning(f"Network error for fileid={fileid}: {e}. Retrying in {backoff_time}s...")
                        await asyncio.sleep(backoff_time)
                    else:
                        LOGGER.error(f"Failed to resolve video links for fileid={fileid} after {self.max_retries+1} attempts: {e}")
                        return []
                        
            LOGGER.warning(f"Could not extract any video links for fileid={fileid}")
            return []
            
        except Exception as e:
            LOGGER.error(f"Unexpected error in get_video_links for fileid={fileid}: {e}", exc_info=True)
            return []
            
    async def _submit_form_and_get_links(self, session: aiohttp.ClientSession, fileid: str) -> List[Dict[str, str]]:
        """
        Submit the form with the fileid and extract video links.
        
        Args:
            session: Aiohttp client session
            fileid: File ID to retrieve
            
        Returns:
            List of video file dictionaries
        """
        form_data = {"fileid": fileid}
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0",
            "Referer": self.base_url,
            "Range": f"bytes=0-{MAX_VIDEO_SIZE}"  # Limit download size
        }
        
        # Step 1: Send the POST request without following redirects
        async with session.post(
            self.post_url,
            data=form_data,
            headers=headers,
            allow_redirects=False,  # Important: don't follow redirects automatically
            timeout=15
        ) as response:
            # Check for size limit
            if 'Content-Length' in response.headers:
                try:
                    content_length = int(response.headers['Content-Length'])
                    if content_length > MAX_VIDEO_SIZE:
                        LOGGER.warning(f"Video size exceeds limit: {content_length} bytes")
                        # Continue with range request anyway
                except (ValueError, TypeError):
                    pass
            
            # Check for redirect response
            if response.status in (301, 302, 303, 307, 308):
                return await self._handle_redirect(session, response, fileid)
                
            # Handle direct (non-redirect) response
            html = await response.text()
            return self._extract_links_from_html(html, fileid)
    
    async def _handle_redirect(self, session: aiohttp.ClientSession, response: aiohttp.ClientResponse, fileid: str) -> List[Dict[str, str]]:
        """
        Handle redirect responses to find video links.
        
        Args:
            session: Aiohttp client session
            response: The redirect response
            fileid: Original file ID for logging
            
        Returns:
            List of video file dictionaries
        """
        redirect_url = response.headers.get('Location', '')
        
        # Case 1: Direct MP4 redirect (simplest case)
        if redirect_url.endswith('.mp4'):
            # Ensure URL is absolute
            if not redirect_url.startswith('http'):
                redirect_url = f"{self.base_url}/{redirect_url.lstrip('/')}"
                
            quality = extract_quality_from_url(redirect_url)
            LOGGER.info(f"Direct MP4 redirect detected: {quality}p -> {redirect_url}")
            
            return [{
                "quality": quality,
                "url": redirect_url,
                "mirror_url": None,
                "size": ""
            }]
        
        # Case 2: Redirect to an intermediate page that contains the MP4 link
        if redirect_url:
            LOGGER.info(f"Following redirect to intermediate page: {redirect_url}")
            try:
                headers = {"Range": f"bytes=0-{MAX_VIDEO_SIZE}"}  # Limit download size
                async with session.get(redirect_url, headers=headers, timeout=15) as page_response:
                    if page_response.status != 200:
                        LOGGER.warning(f"Redirect page returned status {page_response.status}")
                        return []
                    
                    html = await page_response.text()
                    return self._extract_links_from_html(html, fileid)
                    
            except aiohttp.ClientError as e:
                LOGGER.error(f"Error following redirect for fileid={fileid}: {e}")
                return []
                
        # No redirect URL found
        LOGGER.warning(f"Redirect response without Location header for fileid={fileid}")
        return []
    
    def _extract_links_from_html(self, html: str, fileid: str) -> List[Dict[str, str]]:
        """
        Extract video links from HTML content.
        
        Args:
            html: HTML content to parse
            fileid: Original file ID for logging
            
        Returns:
            List of video file dictionaries
        """
        if not html:
            LOGGER.warning(f"Empty HTML response for fileid={fileid}")
            return []
            
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            # Find primary and mirror MP4 links
            primary_url = None
            mirror_url = None
            
            # Try each selector in order of specificity
            for selector in MP4_SELECTORS:
                links = soup.select(selector)
                for i, a in enumerate(links):
                    href = a.get("href")
                    if href:
                        # Normalize URL if it's relative
                        if not href.startswith('http'):
                            href = f"{self.base_url}/{href.lstrip('/')}"
                            
                        if not primary_url:
                            primary_url = href
                        elif not mirror_url:
                            mirror_url = href
                            break  # Found both primary and mirror
                
                # If we found a primary URL, no need to try further selectors
                if primary_url:
                    break
            
            if not primary_url:
                LOGGER.warning(f"No MP4 links found in HTML for fileid={fileid}")
                return []
                
            # Extract quality from the primary URL
            quality = extract_quality_from_url(primary_url)
            
            LOGGER.info(f"Found video link: {quality}p -> {primary_url}")
            if mirror_url:
                LOGGER.info(f"Found mirror link: {mirror_url}")
                
            return [{
                "quality": quality,
                "url": primary_url,
                "mirror_url": mirror_url,
                "size": ""
            }]
            
        except Exception as e:
            LOGGER.error(f"Error extracting links from HTML for fileid={fileid}: {e}")
            return []


def extract_quality_from_url(url: str) -> str:
    """
    Extract video quality from URL.
    
    Args:
        url: URL to analyze
        
    Returns:
        Quality string (e.g., "720", "1080") or "unknown" if not found
    """
    if not url:
        return "unknown"
        
    # Normalize URL for consistency
    url_lower = url.lower()
    
    # Check for flnd.buzz URLs (farsiland CDN)
    if 'flnd.buzz' in url_lower:
        # Check for HD/SD indicators
        if any(marker in url_lower for marker in ['hd', '720', '1080']):
            return "720"
        return "480"  # Default to standard definition
    
    # Step 1: Try regex patterns
    for pattern in QUALITY_PATTERNS:
        match = re.search(pattern, url)
        if match:
            quality = match.group(1)
            # Validate if the extracted quality is realistic
            if quality in VALID_QUALITIES:
                return quality
            # For non-standard values, try to map to nearest standard
            try:
                q_num = int(quality)
                if 1 <= q_num <= 360:
                    return "360"
                elif 361 <= q_num <= 480:
                    return "480"
                elif 481 <= q_num <= 720:
                    return "720"
                elif 721 <= q_num <= 1080:
                    return "1080"
                elif 1081 <= q_num <= 1440:
                    return "1440"
                elif 1441 <= q_num <= 2160:
                    return "2160"
                elif q_num > 2160:
                    return "4320"
            except ValueError:
                # Not a valid number, continue to next pattern
                continue
    
    # Step 2: Look for quality phrases
    for phrase, value in QUALITY_PHRASES.items():
        if phrase in url_lower:
            return value
    
    # Step 3: Fallback - try to find any 3-4 digit number that might be a resolution
    fallback_match = re.search(r'[^0-9](\d{3,4})[^0-9]', url)
    if fallback_match:
        try:
            quality = fallback_match.group(1)
            q_num = int(quality)
            if 360 <= q_num <= 4320:
                # Find the closest standard quality
                for valid in sorted([int(q) for q in VALID_QUALITIES]):
                    if q_num <= valid:
                        return str(valid)
        except (ValueError, IndexError):
            pass
    
    # No quality found
    return "unknown"


# For backwards compatibility
async def get_video_links_from_form(session: aiohttp.ClientSession, fileid: str) -> Optional[List[Dict[str, str]]]:
    """
    Backwards-compatible wrapper for the VideoLinkResolver class.
    
    Args:
        session: Aiohttp client session
        fileid: File ID to retrieve
        
    Returns:
        List of video file dictionaries or None if error
    """
    resolver = VideoLinkResolver()
    try:
        links = await resolver.get_video_links(session, fileid)
        return links if links else None
    except Exception as e:
        LOGGER.error(f"Error in get_video_links_from_form for fileid={fileid}: {e}")
        return None