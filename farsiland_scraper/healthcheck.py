#!/usr/bin/env python3
# File: farsiland_scraper/healthcheck.py
# Version: 1.0.0
# Last Updated: 2025-06-01

"""
Health check for Docker container.
"""
import os
import sys
import sqlite3
from pathlib import Path

# Import config without loading the entire application
try:
    from farsiland_scraper.config import DATABASE_PATH, LOG_DIR
except ImportError:
    # Fallback paths
    DATABASE_PATH = "/data/farsiland.db"
    LOG_DIR = "/logs"

def check_database():
    """Check if database is accessible and contains tables."""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check for tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        # Run integrity check
        cursor.execute("PRAGMA quick_check;")
        integrity = cursor.fetchone()
        
        conn.close()
        
        # Check if we have tables and integrity is OK
        return len(tables) > 0 and integrity[0] == "ok"
    except Exception:
        return False

def check_logs():
    """Check if log directory is writable."""
    try:
        log_path = Path(LOG_DIR)
        test_file = log_path / "healthcheck.tmp"
        with open(test_file, 'w') as f:
            f.write("test")
        os.unlink(test_file)
        return True
    except Exception:
        return False

def check_network():
    """Check if network is available."""
    try:
        import socket
        socket.create_connection(("8.8.8.8", 53), timeout=5)
        return True
    except Exception:
        return False

if __name__ == "__main__":
    db_ok = check_database()
    logs_ok = check_logs()
    network_ok = check_network()
    
    if db_ok and logs_ok and network_ok:
        print("Health check passed")
        sys.exit(0)
    else:
        print(f"Health check failed: database={db_ok}, logs={logs_ok}, network={network_ok}")
        sys.exit(1)