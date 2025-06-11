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

# Copy all binance files
COPY . .

# Create logs directory
RUN mkdir -p /app/logs

# Set timezone
ENV TZ=Asia/Kolkata
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Health check script
RUN echo '#!/bin/bash\npgrep -f run_connector.py > /dev/null' > /app/healthcheck.sh && chmod +x /app/healthcheck.sh

# Run binance connector with flags
CMD ["python", "run_connector.py", "--all-symbols", "--disable-prints"]