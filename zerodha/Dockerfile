FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers and dependencies
RUN playwright install
RUN playwright install-deps

# Copy all zerodha files
COPY . .

# Create logs directory
RUN mkdir -p /app/logs

# Set timezone
ENV TZ=Asia/Kolkata
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Health check script
RUN echo '#!/bin/bash\npgrep -f scheduler.py > /dev/null' > /app/healthcheck.sh && chmod +x /app/healthcheck.sh

# Run the scheduler
CMD ["python", "scheduler.py"]