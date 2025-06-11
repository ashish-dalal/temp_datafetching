#!/bin/bash

# Zerodha & Binance Trading System Setup Script
echo "Setting up Zerodha & Binance Trading System..."

# Create log directories
echo "Creating log directories..."
mkdir -p binance/docker-logs
mkdir -p zerodha/docker-logs

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "ERROR: .env file not found!"
    echo "Please create a .env file with your Zerodha and Binance credentials"
    echo "Required variables:"
    echo "  - ZERODHA_API_KEY"
    echo "  - ZERODHA_API_SECRET" 
    echo "  - ZERODHA_ACCESS_TOKEN"
    echo "  - QUESTDB_HOST"
    echo "  - QUESTDB_PORT"
    echo "  - QUESTDB_USER"
    echo "  - QUESTDB_PASSWORD"
    exit 1
fi

echo "Found .env file"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "ERROR: Docker is not installed!"
    echo "Please install Docker first: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "ERROR: Docker Compose is not installed!"
    echo "Please install Docker Compose first: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "Docker and Docker Compose are installed"

# Build and start the containers
echo "Building Docker containers..."
docker-compose build

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to build Docker containers"
    exit 1
fi

echo "Docker containers built successfully"

echo "Starting trading system..."
docker-compose up -d

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to start containers"
    exit 1
fi

echo "Trading system started successfully!"

# Show container status
echo ""
echo "Container Status:"
docker-compose ps

echo ""
echo "Useful Commands:"
echo "  View logs:           docker-compose logs -f"
echo "  View Binance logs:   docker-compose logs -f binance"
echo "  View Zerodha logs:   docker-compose logs -f zerodha"
echo "  Stop system:         docker-compose down"
echo "  Restart system:      docker-compose restart"
echo "  Check status:        docker-compose ps"

echo ""
echo "System is now running!"
echo "   - Binance: Collecting data 24/7"
echo "   - Zerodha: Will start token renewal at 8:00 AM IST, then collect data from 9:10 AM IST to 3:35 PM IST"
echo "   - Logs: Available in logs/binance/docker-logs/ and logs/zerodha/docker-logs/"