# File: farsiland_scraper/database/migrations.py
# Updated: 2025-04-27
# Changelog:
# - Make AddLastAiredToShows.up idempotent by checking for existing 'last_aired' column before adding it.

"""
Database schema migrations for Farsiland scraper.

Handles schema versioning and automatic migrations to keep
the database schema in sync with code changes.
"""

import os
import sqlite3
import logging
from typing import Dict, List, Optional, Any, Tuple

from farsiland_scraper.config import LOGGER, DATABASE_PATH

class Migration:
    """Base class for migrations."""
    
    version: int = 0
    description: str = ""
    
    @staticmethod
    def up(conn: sqlite3.Connection) -> bool:
        """
        Apply migration.
        
        Args:
            conn: SQLite connection
            
        Returns:
            True if successful
        """
        raise NotImplementedError("Migration.up() must be implemented by subclasses")
        
    @staticmethod
    def down(conn: sqlite3.Connection) -> bool:
        """
        Revert migration.
        
        Args:
            conn: SQLite connection
            
        Returns:
            True if successful
        """
        raise NotImplementedError("Migration.down() must be implemented by subclasses")

class AddDescriptionToEpisodes(Migration):
    """Add description field to episodes table."""
    
    version = 4
    description = "Add description field to episodes table"
    
    @staticmethod
    def up(conn: sqlite3.Connection) -> bool:
        """Add description column to episodes table, if not already present."""
        try:
            cursor = conn.cursor()
            # Check existing columns in 'episodes'
            cursor.execute("PRAGMA table_info(episodes);")
            existing_columns = [row[1] for row in cursor.fetchall()]
            # Only add column if missing
            if "description" not in existing_columns:
                cursor.execute("ALTER TABLE episodes ADD COLUMN description TEXT;")
                conn.commit()
            return True
        except Exception as e:
            LOGGER.error(f"Migration {AddDescriptionToEpisodes.version} failed: {e}")
            conn.rollback()
            return False
            
    @staticmethod
    def down(conn: sqlite3.Connection) -> bool:
        """Cannot remove columns in SQLite."""
        LOGGER.warning("SQLite does not support removing columns")
        return True
        
class CreateSchemaVersionTable(Migration):
    """Initial migration to create schema version table."""
    
    version = 1
    description = "Create schema version table"
    
    @staticmethod
    def up(conn: sqlite3.Connection) -> bool:
        """Create schema version table."""
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    description TEXT
                )
            """)
            conn.commit()
            return True
        except Exception as e:
            LOGGER.error(f"Migration {CreateSchemaVersionTable.version} failed: {e}")
            conn.rollback()
            return False
            
    @staticmethod
    def down(conn: sqlite3.Connection) -> bool:
        """Drop schema version table."""
        try:
            conn.execute("DROP TABLE IF EXISTS schema_version")
            conn.commit()
            return True
        except Exception as e:
            LOGGER.error(f"Migration {CreateSchemaVersionTable.version} reversion failed: {e}")
            conn.rollback()
            return False


class AddLastAiredToShows(Migration):
    """Add last_aired field to shows table."""
    
    version = 2
    description = "Add last_aired field to shows table"
    
    @staticmethod
    def up(conn: sqlite3.Connection) -> bool:
        """Add last_aired column, if not already present."""
        try:
            cursor = conn.cursor()
            # Check existing columns in 'shows'
            cursor.execute("PRAGMA table_info(shows);")
            existing_columns = [row[1] for row in cursor.fetchall()]
            # Only add column if missing
            if "last_aired" not in existing_columns:
                cursor.execute("ALTER TABLE shows ADD COLUMN last_aired TEXT;")
                conn.commit()
            return True
        except Exception as e:
            LOGGER.error(f"Migration {AddLastAiredToShows.version} failed: {e}")
            conn.rollback()
            return False
            
    @staticmethod
    def down(conn: sqlite3.Connection) -> bool:
        """Cannot remove columns in SQLite, create new table instead."""
        LOGGER.warning("SQLite does not support removing columns")
        return True


class AddLanguageCodeToContent(Migration):
    """Add language_code field to all content tables."""
    
    version = 3
    description = "Add language_code field to content tables"
    
    @staticmethod
    def up(conn: sqlite3.Connection) -> bool:
        """Add language_code field to shows, episodes, and movies tables, if missing.""" 
        try: 
            cursor = conn.cursor() 
            for table in ("shows", "episodes", "movies"): 
                # fetch existing columns 
                cursor.execute(f"PRAGMA table_info({table});") 
                existing = [row[1] for row in cursor.fetchall()] 
 
                if "language_code" not in existing: 
                    conn.execute( 
                        f"ALTER TABLE {table} ADD COLUMN language_code TEXT DEFAULT 'fa'" 
                    ) 
            conn.commit() 
            return True 
        except Exception as e: 
            LOGGER.error(f"Migration {AddLanguageCodeToContent.version} failed: {e}") 
            conn.rollback() 
            return False
            
    @staticmethod
    def down(conn: sqlite3.Connection) -> bool:
        """Cannot remove columns in SQLite."""
        LOGGER.warning("SQLite does not support removing columns")
        return True


# List of all migrations in order
MIGRATIONS = [
    CreateSchemaVersionTable,
    AddLastAiredToShows,
    AddLanguageCodeToContent,
    AddDescriptionToEpisodes,
]


class MigrationManager:
    """
    Manages database schema migrations.
    
    Features:
    - Track current schema version
    - Apply pending migrations
    - Revert applied migrations
    - Generate migration scripts
    """
    
    def __init__(self, db_path: str = DATABASE_PATH):
        """
        Initialize migration manager.
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self.conn = None
        self.logger = LOGGER
        
    def connect(self) -> bool:
        """
        Connect to database.
        
        Returns:
            True if connected successfully
        """
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys=ON")
            return True
        except Exception as e:
            self.logger.error(f"Migration connection error: {e}")
            return False
            
    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_current_version(self) -> int:
        """
        Get current schema version.
        
        Returns:
            Current schema version (0 if none)
        """
        if not self.conn:
            if not self.connect():
                return 0
                
        try:
            cursor = self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            if not cursor.fetchone():
                return 0
                
            cursor = self.conn.execute("SELECT MAX(version) as version FROM schema_version")
            row = cursor.fetchone()
            return row[0] if row and row[0] else 0
            
        except Exception as e:
            self.logger.error(f"Error getting schema version: {e}")
            return 0

    def apply_migrations(self, target_version: Optional[int] = None) -> bool:
        """
        Apply all pending migrations up to target_version.
        
        Args:
            target_version: Version to migrate to (None for latest)
            
        Returns:
            True if successful
        """
        if not self.conn:
            if not self.connect():
                return False
                
        current_version = self.get_current_version()
        if target_version is None:
            target_version = MIGRATIONS[-1].version if MIGRATIONS else 0
                
        if current_version >= target_version:
            self.logger.info(f"Database already at version {current_version}, no migrations needed")
            return True
                
        self.logger.info(f"Migrating database from version {current_version} to {target_version}")
        
        try:
            for migration in MIGRATIONS:
                if migration.version <= current_version:
                    continue
                if migration.version > target_version:
                    break
                
                self.logger.info(f"Applying migration {migration.version}: {migration.description}")
                
                if not migration.up(self.conn):
                    self.logger.error(f"Migration {migration.version} failed")
                    return False
                    
                self.conn.execute(
                    "INSERT INTO schema_version (version, description) VALUES (?, ?)",
                    (migration.version, migration.description)
                )
                self.conn.commit()
                
            self.logger.info(f"Database migrated to version {target_version}")
            return True
            
        except Exception as e:
            self.logger.error(f"Migration error: {e}")
            self.conn.rollback()
            return False

    def revert_migrations(self, target_version: int = 0) -> bool:
        """
        Revert migrations back to target_version.
        
        Args:
            target_version: Version to revert to
            
        Returns:
            True if successful
        """
        if not self.conn:
            if not self.connect():
                return False
                
        current_version = self.get_current_version()
        
        if current_version <= target_version:
            self.logger.info(f"Database already at version {current_version}, no reversion needed")
            return True
                
        self.logger.info(f"Reverting database from version {current_version} to {target_version}")
        
        try:
            for migration in reversed(MIGRATIONS):
                if migration.version <= target_version:
                    break
                if migration.version > current_version:
                    continue
                    
                self.logger.info(f"Reverting migration {migration.version}: {migration.description}")
                
                if not migration.down(self.conn):
                    self.logger.error(f"Migration {migration.version} reversion failed")
                    return False
                    
                self.conn.execute(
                    "DELETE FROM schema_version WHERE version = ?",
                    (migration.version,)
                )
                self.conn.commit()
                
            self.logger.info(f"Database reverted to version {target_version}")
            return True
            
        except Exception as e:
            self.logger.error(f"Migration reversion error: {e}")
            self.conn.rollback()
            return False
