# File: farsiland_scraper/auth/session_manager.py
# Version: 1.0.0
# Last Updated: 2025-06-01

"""
Session management for Farsiland scraper.

Handles login, cookie persistence, and authentication for protected content.
"""

import os
import json
import time
import logging
import aiohttp
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, Any

from farsiland_scraper.config import (
    LOGGER,
    CACHE_DIR,
    BASE_URL,
    USER_CREDENTIALS,
    DEFAULT_HEADERS
)

class SessionManager:
    """
    Manages authentication state and cookie persistence.
    
    Features:
    - Login to the website
    - Store and reuse cookies
    - Handle session expiration
    - Protect against site protection mechanisms
    """
    
    def __init__(self, username=None, password=None, cookie_file=None):
        """Initialize session manager with credentials."""
        self.username = username or os.environ.get('FARSILAND_USERNAME') or USER_CREDENTIALS.get('username')
        self.password = password or os.environ.get('FARSILAND_PASSWORD') or USER_CREDENTIALS.get('password')
        self.cookie_file = cookie_file or os.path.join(CACHE_DIR, "session_cookies.json")
        self.session: Optional[aiohttp.ClientSession] = None
        self.logged_in = False
        self.last_login = None
        self.cookies = {}
        self.user_agents = self._load_user_agents()
        self.current_user_agent_index = 0
        self.logger = LOGGER
        
        # Load cookies if they exist
        self._load_cookies()
        
    def _load_user_agents(self) -> list:
        """Load user agent strings for rotation."""
        return [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1"
        ]
        
    def get_next_user_agent(self) -> str:
        """Get the next user agent in the rotation."""
        agent = self.user_agents[self.current_user_agent_index]
        self.current_user_agent_index = (self.current_user_agent_index + 1) % len(self.user_agents)
        return agent
        
    def _load_cookies(self) -> None:
        """Load cookies from disk if they exist."""
        if os.path.exists(self.cookie_file):
            try:
                with open(self.cookie_file, 'r') as f:
                    data = json.load(f)
                    self.cookies = data.get('cookies', {})
                    self.last_login = data.get('last_login')
                    
                    # Check if cookies are expired
                    if self.last_login:
                        last_login_time = datetime.fromisoformat(self.last_login)
                        if datetime.now() - last_login_time > timedelta(days=1):
                            self.logger.info("Stored cookies are expired, will need to login again")
                            self.cookies = {}
                            self.last_login = None
                        else:
                            self.logged_in = True
                            self.logger.info("Loaded valid session cookies")
            except Exception as e:
                self.logger.error(f"Error loading cookies: {e}")
                self.cookies = {}
                
    def _save_cookies(self) -> None:
        """Save cookies to disk for persistence."""
        try:
            os.makedirs(os.path.dirname(self.cookie_file), exist_ok=True)
            
            with open(self.cookie_file, 'w') as f:
                json.dump({
                    'cookies': self.cookies,
                    'last_login': self.last_login
                }, f)
                
            self.logger.info("Saved session cookies to disk")
        except Exception as e:
            self.logger.error(f"Error saving cookies: {e}")
            
    async def ensure_session(self) -> aiohttp.ClientSession:
        """
        Get or create an authenticated session.
        
        Returns:
            An active aiohttp.ClientSession with authentication
        """
        if self.session is None:
            # Create new session
            headers = DEFAULT_HEADERS.copy()
            headers['User-Agent'] = self.get_next_user_agent()
            
            self.session = aiohttp.ClientSession(
                cookies=self.cookies,
                headers=headers
            )
            
        # Check if we need to login
        if not self.logged_in:
            await self.login()
            
        return self.session
    
    def get_session(self):
        """
        Synchronous method to get a session (use when awaiting is not possible).
        
        Returns:
            A requests.Session with cookies (not aiohttp.ClientSession)
        """
        # Synchronous alternative - create and return a requests session
        import requests
        session = requests.Session()
        
        # Apply headers
        headers = DEFAULT_HEADERS.copy()
        headers['User-Agent'] = self.get_next_user_agent()
        session.headers.update(headers)
        
        # Apply cookies
        for cookie_name, cookie_value in self.cookies.items():
            session.cookies.set(cookie_name, cookie_value)
        
        return session
    
    async def login(self) -> bool:
        """
        Login to the website.
        
        Returns:
            True if login successful, False otherwise
        """
        if not self.username or not self.password:
            self.logger.warning("No login credentials provided, skipping login")
            return False
            
        try:
            if self.session is None:
                await self.ensure_session()
                
            self.logger.info(f"Logging in as {self.username}")
            
            # First visit home page to get any CSRF tokens
            await self.session.get(f"{BASE_URL}/")
            
            # Perform login
            login_data = {
                'username': self.username,
                'password': self.password,
                'action': 'login',
                'remember': '1'
            }
            
            login_url = f"{BASE_URL}/wp-login.php"
            async with self.session.post(login_url, data=login_data, allow_redirects=True) as response:
                if response.status != 200:
                    self.logger.error(f"Login failed with status {response.status}")
                    return False
                    
                # Check if login was successful
                content = await response.text()
                if "login_error" in content or "incorrect" in content.lower():
                    self.logger.error("Login failed: Invalid credentials")
                    return False
                    
                # Get cookies
                for cookie_name, cookie in self.session.cookie_jar.filter_cookies(BASE_URL).items():
                    self.cookies[cookie_name] = cookie.value
                    
                self.logged_in = True
                self.last_login = datetime.now().isoformat()
                
                # Save cookies for future use
                self._save_cookies()
                
                self.logger.info("Login successful")
                return True
                
        except Exception as e:
            self.logger.error(f"Login error: {e}")
            self.logged_in = False
            return False
            
    async def close(self) -> None:
        """Close the session and save cookies."""
        if self.session:
            # Get final cookies
            for cookie_name, cookie in self.session.cookie_jar.filter_cookies(BASE_URL).items():
                self.cookies[cookie_name] = cookie.value
                
            # Save cookies
            self._save_cookies()
            
            # Close session
            await self.session.close()
            self.session = None
    
    def __del__(self):
        """Cleanup when object is deleted."""
        if self.session and not self.session.closed:
            # Cannot create asyncio task in __del__
            LOGGER.warning("Session was not properly closed")