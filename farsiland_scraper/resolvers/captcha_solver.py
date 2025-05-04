# File: farsiland_scraper/resolvers/captcha_solver.py
# Version: 1.0.0
# Last Updated: 2025-06-01

"""
CAPTCHA detection and handling for Farsiland scraper.

Provides mechanisms to:
1. Detect various CAPTCHA types
2. Notify users of CAPTCHA challenges
3. Implement simple automatic solving strategies
4. Integrate with crawler to pause/resume on CAPTCHA detection
"""

import os
import time
import logging
import base64
import tempfile
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, Union

from farsiland_scraper.config import LOGGER

class CaptchaDetectedException(Exception):
    """Exception raised when a CAPTCHA is detected."""
    def __init__(self, url: str, captcha_type: str = "unknown"):
        self.url = url
        self.captcha_type = captcha_type
        super().__init__(f"CAPTCHA detected ({captcha_type}) at {url}")

class CaptchaSolver:
    """
    Detect and handle various types of CAPTCHAs.
    
    Features:
    - Detection of common CAPTCHA types
    - Simple solving mechanisms
    - Integration with pause/resume workflow
    """
    
    # CAPTCHA detection patterns
    CAPTCHA_PATTERNS = {
        'recaptcha': [
            'google.com/recaptcha',
            'g-recaptcha',
            'grecaptcha',
            'recaptcha/api.js'
        ],
        'hcaptcha': [
            'hcaptcha.com',
            'h-captcha',
            'hcaptcha-widget'
        ],
        'simple': [
            'captcha',
            'security code',
            'security image',
            'verify you are human',
            'verification required'
        ]
    }
    
    def __init__(self, notification_dir: Optional[str] = None, pause_on_captcha: bool = True):
        """
        Initialize CAPTCHA solver.
        
        Args:
            notification_dir: Directory to save CAPTCHA screenshots
            pause_on_captcha: Whether to pause crawling on CAPTCHA detection
        """
        self.logger = LOGGER
        self.notification_dir = notification_dir or os.path.join(Path.home(), "captcha_notifications")
        os.makedirs(self.notification_dir, exist_ok=True)
        self.pause_on_captcha = pause_on_captcha
        
    def detect_captcha(self, html: str) -> Tuple[bool, str]:
        """
        Detect if HTML contains a CAPTCHA.
        
        Args:
            html: HTML content to check
            
        Returns:
            Tuple of (is_captcha, captcha_type)
        """
        html_lower = html.lower()
        
        # Check for each CAPTCHA type
        for captcha_type, patterns in self.CAPTCHA_PATTERNS.items():
            for pattern in patterns:
                if pattern.lower() in html_lower:
                    self.logger.warning(f"Detected {captcha_type} CAPTCHA")
                    return True, captcha_type
                    
        return False, ""
        
    def handle_captcha(self, url: str, html: str, session=None) -> bool:
        """
        Handle detected CAPTCHA.
        
        Args:
            url: The URL with the CAPTCHA
            html: HTML content containing the CAPTCHA
            session: Optional active session
            
        Returns:
            True if CAPTCHA was handled, False otherwise
            
        Raises:
            CaptchaDetectedException if pause_on_captcha is True
        """
        is_captcha, captcha_type = self.detect_captcha(html)
        
        if not is_captcha:
            return False
            
        # Save CAPTCHA notification
        notification_file = os.path.join(
            self.notification_dir,
            f"captcha_{int(time.time())}_{captcha_type}.html"
        )
        
        try:
            with open(notification_file, 'w', encoding='utf-8') as f:
                f.write(f"<!-- CAPTCHA detected at {url} -->\n")
                f.write(html)
                
            self.logger.warning(f"CAPTCHA notification saved to {notification_file}")
            
            # Take screenshot if session supports it
            if session and hasattr(session, 'screenshot'):
                try:
                    screenshot = session.screenshot()
                    screenshot_path = notification_file.replace('.html', '.png')
                    with open(screenshot_path, 'wb') as f:
                        f.write(screenshot)
                except Exception as e:
                    self.logger.error(f"Failed to take screenshot: {e}")
            
            # Raise exception to pause crawler if configured
            if self.pause_on_captcha:
                raise CaptchaDetectedException(url, captcha_type)
                
            return True
            
        except (CaptchaDetectedException, OSError) as e:
            # Don't catch CaptchaDetectedException, but catch file errors
            if isinstance(e, OSError):
                self.logger.error(f"Error saving CAPTCHA notification: {e}")
            raise
            
        except Exception as e:
            self.logger.error(f"Error handling CAPTCHA: {e}")
            return False
            
    def solve_simple_captcha(self, html: str) -> Optional[Dict[str, str]]:
        """
        Try to solve simple text/image CAPTCHAs.
        
        Args:
            html: HTML containing the CAPTCHA
            
        Returns:
            Dict with form field values if solved, None otherwise
        """
        # This is a placeholder - actual implementation would be more complex
        # and potentially integrate with external services
        self.logger.info("Simple CAPTCHA solving not implemented")
        return None