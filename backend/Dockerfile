FROM python:3.11-slim

# Set working directory
WORKDIR /code

# Install system dependencies (GDAL, Postgres client, etc.)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    gdal-bin \
    libgdal-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Set GDAL path (important)
ENV GDAL_LIBRARY_PATH=/usr/lib/libgdal.so

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DJANGO_SETTINGS_MODULE=config.settings

# Let docker-compose handle the command
