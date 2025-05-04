# File: Dockerfile
# Version: 1.1.0
# Last Updated: 2025-05-25

# Use Python 3.12 as the base image with exact version pinning
FROM python:3.12.5-slim-bullseye

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories with correct permissions
RUN mkdir -p /app/data /app/logs /app/cache && \
    chmod -R 777 /app/data /app/logs /app/cache

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Create a non-root user to run the application
RUN useradd -m scraper && \
    chown -R scraper:scraper /app

# Switch to the non-root user
USER scraper

# Expose volume for persistent data
VOLUME ["/app/data", "/app/logs", "/app/cache"]

# Default command
CMD ["python", "-m", "farsiland_scraper.run", "--daemon", "--rss", "--api-first", "--notify"]

# Health check
HEALTHCHECK --interval=5m --timeout=30s --retries=3 \
    CMD python -c "import os; import time; exit(0 if os.path.exists('/app/data/site_index.json') and time.time() - os.path.getmtime('/app/data/site_index.json') < 3600 else 1)" || exit 1