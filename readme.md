# Farsiland Scraper

A high-performance web scraper for extracting and monitoring content from Farsiland.com, featuring API-first architecture with HTML fallback, RSS monitoring, incremental updates, and Docker/NAS compatibility.

**Current Version**: 8.0.0 (June 2, 2025)

![Docker Compatible](https://img.shields.io/badge/Docker-Compatible-blue)
![NAS Compatible](https://img.shields.io/badge/NAS-Compatible-green)
![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-brightgreen)

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Installation](#installation)
  - [Standard Installation](#standard-installation)
  - [Docker Installation](#docker-installation)
  - [NAS Installation](#nas-installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Basic Usage](#basic-usage)
  - [Advanced Usage](#advanced-usage)
  - [Docker Usage](#docker-usage)
  - [Command Reference](#command-reference)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)
- [Performance Optimization](#performance-optimization)
- [Maintenance](#maintenance)

## Overview

Farsiland Scraper is a specialized tool for extracting and monitoring movies, TV shows, and episodes from Farsiland.com. It uses a hybrid approach with WordPress API as the primary data source and HTML scraping as a fallback, ensuring maximum reliability and performance.

The scraper extracts detailed information about:
- **Movies**: Title (English & Farsi), description, poster, release date, cast, genres, and download links
- **TV Shows/Series**: Title, description, poster, seasons, episode count, and metadata
- **Episodes**: Episode information, thumbnails, and download links

Data is stored in an SQLite database and can be exported to JSON format for integration with other systems.

## Key Features

### Core Features
- **API-First Architecture**: Uses WordPress REST API as the primary data source with HTML fallback
- **RSS Monitoring**: Real-time updates by monitoring RSS feeds
- **Incremental Updates**: Efficiently updates only recently modified content
- **Error Tracking**: Monitors problematic URLs and prevents repeated failures
- **Video Link Resolution**: Advanced video link extraction and quality detection
- **Database Storage**: Persistent storage in SQLite with proper relationships
- **JSON Export**: Export capability for integration with other systems

### Advanced Features
- **Docker & NAS Compatibility**: Optimized for containerized and NAS environments
- **Checkpoint/Resume**: Save progress and resume interrupted crawls
- **Persian Text Support**: Proper handling of non-ASCII/Persian characters in URLs and content
- **Memory Optimization**: Adaptive resource management for constrained environments
- **Authentication**: Support for authenticated content access
- **Schema Migrations**: Automatic database schema updates
- **CAPTCHA Detection**: Recognition and handling of CAPTCHA challenges
- **Proxy Support**: Integration with HTTP/HTTPS proxies
- **Atomic File Operations**: Cross-platform file locking for data integrity

### Performance Features
- **Response Caching**: Reduces API calls for better performance
- **Rate Limiting**: Respects API rate limits to prevent throttling
- **Batch Processing**: Efficient handling of large data sets
- **Adaptive Resource Allocation**: Adjusts resource usage based on environment
- **Parallel Processing**: Configurable concurrency for faster scraping

## Installation

### Standard Installation

#### Prerequisites
- Python 3.9 or higher
- pip package manager

#### Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/farsiland-scraper.git
cd farsiland-scraper
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create required directories:
```bash
mkdir -p data logs cache
```

### Docker Installation

#### Prerequisites
- Docker
- Docker Compose (optional, but recommended)

#### Setup with Docker Compose

1. Clone the repository:
```bash
git clone https://github.com/yourusername/farsiland-scraper.git
cd farsiland-scraper
```

2. Create data directories:
```bash
mkdir -p data logs cache
```

3. Run with Docker Compose:
```bash
docker-compose up -d
```

#### Manual Docker Setup

1. Build the Docker image:
```bash
docker build -t farsiland-scraper .
```

2. Create a container:
```bash
docker run -d \
  --name farsiland-scraper \
  -v $(pwd)/data:/data \
  -v $(pwd)/logs:/logs \
  -v $(pwd)/cache:/cache \
  -e FARSILAND_MAX_MEMORY_MB=1024 \
  -e FARSILAND_MAX_CPU_PERCENT=80 \
  farsiland-scraper \
  --daemon --rss --api-first --update-sitemap
```

### NAS Installation

#### Synology NAS

1. Create a shared folder for the scraper
2. Enable SSH on your Synology NAS
3. Connect via SSH and install Docker
4. Follow the Docker installation steps above
5. Create a scheduled task to run the update script:

```bash
# Create update script
cat > /volume1/docker/farsiland/update.sh << 'EOF'
#!/bin/bash
cd /volume1/docker/farsiland
docker-compose run --rm farsiland-scraper \
  --update-sitemap \
  --sitemap \
  --rss \
  --modified-window 1 \
  --api-first \
  --low-memory
EOF

# Make it executable
chmod +x /volume1/docker/farsiland/update.sh
```

#### QNAP NAS

Follow similar steps as Synology, but use Container Station instead.

## Configuration

The scraper is configured through the `config.py` file and environment variables.

### Key Configuration Options

| Setting | Description | Default | Environment Variable |
|---------|-------------|---------|---------------------|
| `BASE_URL` | Base URL for the website | https://farsiland.com | FARSILAND_BASE_URL |
| `API_BASE_URL` | Base URL for the WordPress API | https://farsiland.com/wp-json/wp/v2 | FARSILAND_API_BASE_URL |
| `DATABASE_PATH` | Path to SQLite database file | ./data/farsiland.db | FARSILAND_DB_PATH |
| `SCRAPE_INTERVAL` | Interval between scrape runs (seconds) | 600 | FARSILAND_SCRAPE_INTERVAL |
| `MAX_ITEMS_PER_CATEGORY` | Maximum items per category | 20000 | FARSILAND_MAX_ITEMS |
| `RSS_POLL_INTERVAL` | Interval between RSS checks (seconds) | 600 | FARSILAND_RSS_POLL_INTERVAL |
| `API_CACHE_ENABLED` | Enable/disable API caching | true | FARSILAND_API_CACHE_ENABLED |
| `API_CACHE_TTL` | Time-to-live for cached API (seconds) | 3600 | FARSILAND_API_CACHE_TTL |
| `ERROR_BLACKLIST_THRESHOLD` | Error threshold before blacklisting | 3 | FARSILAND_ERROR_BLACKLIST_THRESHOLD |
| `MAX_MEMORY_MB` | Memory limit in MB | 1024 | FARSILAND_MAX_MEMORY_MB |
| `MAX_CPU_PERCENT` | CPU usage limit percentage | 80 | FARSILAND_MAX_CPU_PERCENT |
| `MAX_CONCURRENT_PROCESSES` | Maximum concurrent processes | 4 | FARSILAND_MAX_CONCURRENT |
| `PROXY_URL` | HTTP/HTTPS proxy URL | None | FARSILAND_PROXY |
| `MAX_VIDEO_SIZE` | Maximum video size to download (bytes) | 524288000 | FARSILAND_MAX_VIDEO_SIZE |

### Authentication Configuration

To use authentication for accessing protected content:

```bash
export FARSILAND_USERNAME="your_username"
export FARSILAND_PASSWORD="your_password"
```

### Proxy Configuration

To use a proxy server:

```bash
export FARSILAND_PROXY="http://user:pass@proxy:port"
export FARSILAND_PROXY_USER="username"
export FARSILAND_PROXY_PASS="password"
```

## Usage

### Basic Usage

#### Run a one-time scrape with API-first approach:

```bash
python -m farsiland_scraper.run
```

#### Run with HTML-first approach (legacy mode):

```bash
python -m farsiland_scraper.run --html-first
```

#### Run continuously with daemon mode:

```bash
python -m farsiland_scraper.run --daemon
```

#### Run with RSS monitoring:

```bash
python -m farsiland_scraper.run --daemon --rss
```

### Advanced Usage

#### Incremental Updates

Only process content modified since a specific date:

```bash
python -m farsiland_scraper.run --since 2025-04-01
```

Only process content modified in the last N days:

```bash
python -m farsiland_scraper.run --modified-window 7
```

#### Content Selection

Scrape specific content types:

```bash
python -m farsiland_scraper.run --spiders series episodes
python -m farsiland_scraper.run --spiders movies
```

#### Sitemap Options

Use sitemap URLs instead of default URLs:

```bash
python -m farsiland_scraper.run --sitemap
```

Update sitemap before crawling:

```bash
python -m farsiland_scraper.run --update-sitemap
```

Only parse sitemap without fetching pages:

```bash
python -m farsiland_scraper.run --sitemap-only
```

#### Resource Management

Enable low memory mode for NAS/constrained environments:

```bash
python -m farsiland_scraper.run --low-memory
```

Optimize database:

```bash
python -m farsiland_scraper.run --optimize-db
```

Set checkpoint interval:

```bash
python -m farsiland_scraper.run --checkpoint-interval 300
```

#### Output Options

Export to JSON after scraping:

```bash
python -m farsiland_scraper.run --export
```

Specify export file:

```bash
python -m farsiland_scraper.run --export --export-file ./data/custom_export.json
```

Enable notifications about new content:

```bash
python -m farsiland_scraper.run --notify
```

### Docker Usage

Run the container:

```bash
docker run -d \
  --name farsiland-scraper \
  -v $(pwd)/data:/data \
  -v $(pwd)/logs:/logs \
  -v $(pwd)/cache:/cache \
  -e FARSILAND_MAX_MEMORY_MB=1024 \
  farsiland-scraper \
  --daemon --rss --api-first
```

Run a one-time incremental update:

```bash
docker run --rm \
  -v $(pwd)/data:/data \
  -v $(pwd)/logs:/logs \
  -v $(pwd)/cache:/cache \
  farsiland-scraper \
  --update-sitemap --modified-window 1 --api-first
```

### Command Reference

```
usage: run.py [-h] [--daemon] [--spiders {series,episodes,movies,all} [...]] [--url URL] [--force-refresh] 
              [--sitemap] [--sitemap-only] [--update-sitemap] [--sitemap-file SITEMAP_FILE] [--limit LIMIT] 
              [--concurrent-requests CONCURRENT_REQUESTS] [--download-delay DOWNLOAD_DELAY] 
              [--max-video-size MAX_VIDEO_SIZE] [--export] [--export-file EXPORT_FILE] [--notify] 
              [--verbose] [--log-file LOG_FILE] [--api-first | --html-first] [--rss] 
              [--rss-interval RSS_INTERVAL] [--rss-state-file RSS_STATE_FILE] 
              [--since SINCE | --modified-window MODIFIED_WINDOW] [--low-memory] [--optimize-db] 
              [--checkpoint-interval CHECKPOINT_INTERVAL] [--checkpoint-items CHECKPOINT_ITEMS] [--auth]

Farsiland Scraper

options:
  -h, --help                          show this help message and exit
  --daemon                            Run the scraper continuously
  --spiders {series,episodes,movies,all} [...]
                                      Spiders to run (default: ['all'])
  --url URL                           Scrape a specific URL
  --force-refresh                     Ignore cache and re-fetch all HTML

Sitemap Options:
  --sitemap                           Use parsed sitemap URLs for crawling
  --sitemap-only                      Only parse/update sitemap without fetching pages
  --update-sitemap                    Update sitemap data before crawling
  --sitemap-file SITEMAP_FILE         Path to sitemap file

Output Options:
  --export                            Export database to JSON after scraping
  --export-file EXPORT_FILE           Path to export JSON file
  --notify                            Notify about new content after scraping
  --verbose                           Enable verbose logging
  --log-file LOG_FILE                 Path to log file

API Options:
  --api-first                         Use API-first approach with HTML fallback (default)
  --html-first                        Use HTML-first approach with API fallback

RSS Options:
  --rss                               Enable RSS feed monitoring
  --rss-interval RSS_INTERVAL         RSS polling interval in seconds
  --rss-state-file RSS_STATE_FILE     Path to RSS state file

Incremental Update Options:
  --since SINCE                       Only process content modified since date (ISO format: YYYY-MM-DD)
  --modified-window MODIFIED_WINDOW   Only process content modified in the last N days

Resource Management Options:
  --low-memory                        Enable low memory optimization for NAS/Docker
  --optimize-db                       Run database optimization before scraping
  --checkpoint-interval CHECKPOINT_INTERVAL
                                      Checkpoint interval in seconds (0 to disable)
  --checkpoint-items CHECKPOINT_ITEMS
                                      Number of items before automatic checkpoint

Authentication Options:
  --auth                              Enable authentication (uses credentials from config or env vars)

Limits and throttling:
  --limit LIMIT                       Limit number of items to crawl per spider
  --concurrent-requests CONCURRENT_REQUESTS
                                      Maximum concurrent requests
  --download-delay DOWNLOAD_DELAY     Delay between requests in seconds
  --max-video-size MAX_VIDEO_SIZE     Maximum video size to download in bytes
```

## Project Structure

```
farsiland_scraper/
├── __init__.py
├── run.py                      # Main entry point
├── config.py                   # Configuration settings
├── items.py                    # Item definitions
├── fetch.py                    # HTML fetch utilities
├── healthcheck.py              # Docker health check script
├── settings.py                 # Scrapy settings
├── spiders/                    # Scrapy spiders
│   ├── __init__.py
│   ├── series_spider.py        # TV shows/series spider
│   ├── episodes_spider.py      # Episodes spider
│   ├── movies_spider.py        # Movies spider
├── core/                       # Core components
│   ├── __init__.py
│   ├── api_client.py           # WordPress API client
│   ├── rss_monitor.py          # RSS feed monitor
├── utils/                      # Utilities
│   ├── __init__.py
│   ├── checkpoint_manager.py   # Checkpoint/resume functionality
│   ├── error_tracker.py        # Error tracking and blacklisting
│   ├── new_item_tracker.py     # Tracks new content
│   ├── sitemap_parser.py       # Sitemap parsing utility
│   ├── url_patterns.py         # URL pattern matching
├── resolvers/                  # Link resolvers
│   ├── __init__.py 
│   ├── video_link_resolver.py  # Video link extraction
│   ├── captcha_solver.py       # CAPTCHA detection and handling
├── database/                   # Database handlers
│   ├── __init__.py
│   ├── models.py               # Database models
│   ├── migrations.py           # Schema migration manager
├── auth/                       # Authentication
│   ├── __init__.py
│   ├── session_manager.py      # Session management
├── pipelines/                  # Scrapy pipelines
│   ├── __init__.py
│   ├── json_serialization.py   # JSON serialization pipeline
│   ├── save_to_db.py           # Database storage pipeline
├── tests/                      # Test modules
│   ├── __init__.py
│   ├── test_incremental.py     # Incremental update tests
├── Dockerfile                  # Docker container definition
├── docker-compose.yml          # Docker Compose configuration
├── requirements.txt            # Dependencies
```

## Architecture

### API-First Approach with HTML Fallback

The system attempts to obtain data from the WordPress API first, then falls back to HTML scraping if:
1. The API request fails
2. The API response is incomplete
3. API ID can't be determined

This provides maximum reliability while minimizing bandwidth usage and processing overhead.

### Component Interactions

```
                                     ┌───────────────┐
                                     │     run.py    │
                                     │ (Entry Point) │
                                     └───────┬───────┘
                                             │
                                             ▼
                                     ┌───────────────┐
                                     │ ScrapeManager │
                                     └───────┬───────┘
                       ┌─────────────┬──────┴───────┬────────────┐
                       │             │              │            │
               ┌───────▼───────┐    │      ┌───────▼────────┐   │
               │ SitemapParser │    │      │   RSSMonitor   │   │
               └───────┬───────┘    │      └───────┬────────┘   │
                       │            │              │            │
               ┌───────▼───────┐    │              │      ┌─────▼────────┐
               │  parsed_urls  │    │              │      │ ErrorTracker │
               └───────┬───────┘    │              │      └──────┬───────┘
                       │            │              │             │
                       └───────┐    │              │             │
                               ▼    ▼              ▼             ▼
                          ┌────────────────────────────────────────┐
                          │                Spiders                 │
                          │      (series, episodes, movies)        │
                          └─────────────┬──────────────────────────┘
                                        │
                       ┌───────────────┴─────────────┐
                       │                             │
               ┌───────▼───────┐           ┌────────▼───────┐
               │   APIClient   │    or     │  HTML Parsing  │
               └───────┬───────┘           └────────┬───────┘
                       │                            │
                       │                   ┌────────▼───────┐
                       │                   │VideoLinkResolver│
                       │                   └────────┬───────┘
                       │                            │
                       └────────────┬───────────────┘
                                    │
                          ┌─────────▼─────────┐
                          │    Items (data)   │
                          └─────────┬─────────┘
                                    │
                          ┌─────────▼─────────┐
                          │     Pipelines     │
                          └─────────┬─────────┘
                                    │
                          ┌─────────▼─────────┐
                          │ Database / Export │
                          └───────────────────┘
```

### Database Schema

The SQLite database contains three main tables:
- **shows**: TV shows/series metadata
- **episodes**: Episode information with foreign keys to shows
- **movies**: Movie metadata

API-specific fields are included in all tables:
- **api_id**: WordPress post ID
- **api_source**: Source of the data (e.g., "wp-json/v2")
- **modified_gmt**: Last modified GMT timestamp from API

### RSS Monitoring

The RSS monitor provides real-time updates by polling Farsiland.com's RSS feeds:

1. Periodically checks the RSS feeds for new content
2. Compares entries against a state file to find new items
3. Extracts WordPress post IDs from entries
4. Fetches complete data using the API client
5. Processes content through the same pipeline as regular scraping

### Incremental Updates

Incremental updates minimize bandwidth and processing requirements:

1. Only fetch content modified after a certain date
2. Filter content at the API level using `modified_after` parameter
3. Filter URLs at the sitemap level using `lastmod` timestamps
4. Update state tracking for processed items

## Troubleshooting

### Common Issues

#### API Errors

If you encounter API errors:

1. Check if the WordPress API is properly configured on the site
2. Verify API base URL in configuration
3. Check if the site has rate limiting
4. Try with `--html-first` to use HTML as primary source
5. Check logs for specific error messages

#### RSS Feed Issues

If RSS monitoring isn't working:

1. Verify RSS feed URLs in configuration
2. Check if feeds contain proper GUIDs
3. Examine the RSS state file for corruption
4. Try deleting the state file to reset RSS tracking

#### Database Issues

For database problems:

1. Check database permissions
2. Run database optimization: `--optimize-db`
3. Verify schema with migrations manager
4. Try with `--force-refresh` to clear cache
5. Check for errors in database initialization logs

#### Video Link Extraction Fails

If video links aren't being extracted:

1. Check if the site structure has changed
2. Examine HTML to verify video links exist
3. Check if CAPTCHA protection is active
4. Try updating the video_link_resolver.py patterns
5. Run with `--verbose` to see detailed logs

#### Docker/NAS Performance Issues

If the scraper is using too many resources:

1. Enable low memory mode: `--low-memory`
2. Reduce concurrent requests: `--concurrent-requests 2`
3. Increase download delay: `--download-delay 2`
4. Limit items per run: `--limit 500`
5. Use incremental updates: `--modified-window 1`

### Logging

Enable verbose logging for troubleshooting:

```bash
python -m farsiland_scraper.run --verbose --log-file debug.log
```

### Debugging

For advanced debugging:

1. Set `FARSILAND_LOG_LEVEL=debug` in environment
2. Run with `--verbose` flag
3. Examine the log file for detailed information
4. Use `--url` parameter to debug a specific page
5. Check error tracker state file for blacklisted URLs

## Performance Optimization

### Recommended Settings for Different Environments

#### Standard Environment (Desktop/Server)

```bash
python -m farsiland_scraper.run --api-first --rss --concurrent-requests 8 --download-delay 0.5
```

#### NAS Environment

```bash
python -m farsiland_scraper.run --api-first --rss --sitemap --modified-window 1 --concurrent-requests 4 --download-delay 1 --low-memory --checkpoint-interval 300
```

#### Docker Container

```bash
docker run -d --name farsiland-scraper \
  -v $(pwd)/data:/data \
  -v $(pwd)/logs:/logs \
  -v $(pwd)/cache:/cache \
  -e FARSILAND_MAX_MEMORY_MB=1024 \
  -e FARSILAND_MAX_CPU_PERCENT=50 \
  farsiland-scraper \
  --daemon --rss --api-first --update-sitemap --low-memory
```

### Optimizing Database Performance

1. Run regular optimization:
```bash
python -m farsiland_scraper.run --optimize-db
```

2. Use appropriate journal mode for your storage type:
```bash
# In config.py for network storage
journal_mode = "DELETE"  # Better for NAS/network storage
# For local SSD storage
journal_mode = "WAL"     # Better for local drives
```

3. Configure cache settings based on available memory:
```bash
# In config.py or environment variables
CACHE_SIZE = 4000  # 4MB cache, adjust based on available memory
```

## Maintenance

### Regular Maintenance Tasks

1. **Database optimization** (monthly):
```bash
python -m farsiland_scraper.run --optimize-db
```

2. **Clear old logs** (monthly):
```bash
find logs -name "*.log.*" -type f -mtime +30 -delete
```

3. **Clear API cache** (if issues occur):
```bash
rm -rf cache/api/*
```

4. **Reset error tracking** (if too many URLs are blacklisted):
```bash
rm data/error_patterns.json
```

5. **Backup database** (weekly):
```bash
cp data/farsiland.db data/farsiland_backup_$(date +%Y%m%d).db
```

### Updating the Scraper

1. Pull the latest code:
```bash
git pull origin main
```

2. Update dependencies:
```bash
pip install -r requirements.txt --upgrade
```
3. Run database migrations (automatic on first run):
```bash
python -m farsiland_scraper.run --optimize-db
```

4. Update Docker image (if using Docker):
```bash
docker-compose build
docker-compose up -d
```

---

For further assistance or to report issues, please contact the project maintainer or open an issue on GitHub.