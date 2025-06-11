# Docker-Based Financial Data Pipeline

This repository contains a containerized financial data collection system that fetches data from Binance and Zerodha exchanges and stores it in QuestDB for real-time analysis.

## Architecture Overview

The system consists of two main containers:
- **Binance Container**: Continuously collects cryptocurrency market data (24/7)
- **Zerodha Container**: Collects Indian stock market data with scheduled token renewal

## Docker Components

### Core Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Multi-container orchestration configuration |
| `setup.sh` | One-time server setup (installs Docker, clones repo) |
| `run.sh` | Data pipeline deployment and management |
| `binance/Dockerfile` | Binance connector container definition |
| `zerodha/Dockerfile` | Zerodha connector container definition |
| `zerodha/scheduler.py` | Smart scheduling for market hours and token renewal |

### Container Specifications

**Binance Container:**
- Base Image: `python:3.10-slim`
- Purpose: 24/7 cryptocurrency data collection
- Dependencies: aiohttp, websockets, questdb, python-dotenv
- Command: `python run_connector.py --all-symbols --disable-prints`

**Zerodha Container:**
- Base Image: `python:3.10-slim`
- Purpose: Indian market data collection with token management
- Dependencies: kiteconnect, questdb, playwright, python-dotenv, psutil
- Command: `python scheduler.py`
- Features: Automated token renewal, market hour scheduling

## Getting Started

### First-Time Server Setup

**Prerequisites:**
- Fresh Ubuntu/Debian/CentOS/RHEL server
- GitHub Personal Access Token (for repository access)

**Option 1: With GitHub Token Flag (Recommended for automation)**
```bash
# Download setup script
wget https://raw.githubusercontent.com/ashish-dalal/data_pipeline_for_questdb/main/setup.sh
chmod +x setup.sh

# Run complete setup
./setup.sh --github-token ghp_xxxxxxxxxxxxxxxxxxxx
```

**Option 2: Interactive Setup**
```bash
# Download setup script
wget https://raw.githubusercontent.com/ashish-dalal/data_pipeline_for_questdb/main/setup.sh
chmod +x setup.sh

# Run setup (will prompt for GitHub credentials)
./setup.sh
```

**Option 3: With Username and Token**
```bash
./setup.sh --username myusername --github-token ghp_xxxxxxxxxxxxxxxxxxxx
```

### What setup.sh Does

1. Detects OS (Ubuntu/Debian/CentOS/RHEL)
2. Installs Docker and Docker Compose
3. Clones the repository from GitHub
4. Creates .env template file
5. Verifies project structure
6. Runs the data pipeline (if .env is configured)

### Configuration

After setup, edit the `.env` file with your actual credentials:

```bash
cd datafetching
nano .env
```

Required variables:
```env
# Zerodha API Configuration
ZERODHA_API_KEY=your_actual_api_key
ZERODHA_API_SECRET=your_actual_api_secret
ZERODHA_ACCESS_TOKEN=your_actual_access_token

# QuestDB Configuration
QUESTDB_HOST=your_questdb_host
QUESTDB_PORT=9009
QUESTDB_USER=admin
QUESTDB_PASSWORD=quest
```

## Regular Operations

### Starting the Data Pipeline

**After initial setup or server restart:**
```bash
cd datafetching
./run.sh
```

### What run.sh Does

1. Creates log directories
2. Validates .env file exists
3. Builds Docker containers
4. Starts both connectors
5. Shows service status and management commands

## Management Commands

### Docker Compose Operations

| Command | Description |
|---------|-------------|
| `docker-compose ps` | Show container status |
| `docker-compose logs -f` | View live logs (all services) |
| `docker-compose logs -f binance` | View Binance connector logs |
| `docker-compose logs -f zerodha` | View Zerodha connector logs |
| `docker-compose up -d` | Start all services |
| `docker-compose down` | Stop all services |
| `docker-compose restart` | Restart all services |
| `docker-compose restart binance` | Restart specific service |
| `docker-compose stop` | Stop without removing containers |
| `docker-compose start` | Start stopped containers |
| `docker-compose pull` | Pull latest images |
| `docker-compose build` | Rebuild containers |
| `docker-compose config` | Validate docker-compose.yml |

### Individual Container Testing

**Build Individual Containers:**
```bash
# Build Binance container
docker build -t binance-test ./binance

# Build Zerodha container  
docker build -t zerodha-test ./zerodha
```

**Run Individual Containers:**
```bash
# Test Binance container
docker run --rm \
  -v $(pwd)/.env:/app/.env \
  -v $(pwd)/binance/docker-logs:/app/logs \
  --name binance-test \
  binance-test

# Test Zerodha container
docker run --rm \
  -v $(pwd)/.env:/app/.env \
  -v $(pwd)/zerodha/docker-logs:/app/logs \
  --name zerodha-test \
  zerodha-test
```

**Debug Containers:**
```bash
# Enter running container for debugging
docker exec -it binance-test /bin/bash
docker exec -it zerodha-test /bin/bash

# View container logs
docker logs -f binance-test
docker logs -f zerodha-test

# Stop individual containers
docker stop binance-test
docker stop zerodha-test
```

### System Monitoring

**Container Resource Usage:**
```bash
docker stats
```

**Container Health:**
```bash
# Check health status
docker-compose ps

# Inspect specific container
docker inspect datafetching_binance_1
docker inspect datafetching_zerodha_1
```

**Log Management:**
```bash
# View log files directly
tail -f binance/docker-logs/*.log
tail -f zerodha/docker-logs/*.log

# Clean up old logs
docker-compose down
docker system prune -f
```

## Data Collection Schedule

### Binance Connector
- **Runtime**: 24/7 continuous operation
- **Data**: Cryptocurrency market data for all symbols
- **Output**: Real-time ticks to QuestDB

### Zerodha Connector  
- **Token Renewal**: Daily at 8:00 AM IST
- **Data Collection**: 9:10 AM - 3:35 PM IST (Indian market hours + 5min buffer)
- **Weekend Behavior**: Automatically skips Saturday & Sunday
- **Output**: Stock market ticks to QuestDB

## Troubleshooting

### Common Issues

**Container Won't Start:**
```bash
# Check logs for errors
docker-compose logs [service-name]

# Verify .env file
cat .env

# Check disk space
df -h
```

**Permission Issues:**
```bash
# Fix log directory permissions
chmod 755 binance/docker-logs zerodha/docker-logs

# Add user to docker group (then logout/login)
sudo usermod -aG docker $USER
```

**Network Issues:**
```bash
# Restart Docker daemon
sudo systemctl restart docker

# Remove and recreate containers
docker-compose down
docker-compose up -d
```

**Database Connection Issues:**
```bash
# Test QuestDB connectivity
telnet $QUESTDB_HOST $QUESTDB_PORT

# Check QuestDB status
curl http://$QUESTDB_HOST:9000
```

### Complete Reset

**If you need to start fresh:**
```bash
# Stop everything
docker-compose down

# Remove containers and images
docker system prune -a -f

# Remove project directory
cd ..
rm -rf datafetching

# Re-run setup
./setup.sh --github-token ghp_xxxxxxxxxxxxxxxxxxxx
```

## Production Deployment

### TODO
1. **Configure log rotation** to prevent disk space issues
2. **Set up alerts** for container failures
3. **Add docker images to private registry** for direct run

### Security Best Practices

- Store GitHub tokens securely
- Use environment variables for sensitive data
- Regularly update Docker images
- Monitor log files for unusual activity
- Implement proper firewall rules

## Development

### Making Changes

**Rebuild after code changes:**
```bash
# Rebuild specific service
docker-compose build binance
docker-compose build zerodha

# Restart with new build
docker-compose up -d --force-recreate
```

**Test configuration changes:**
```bash
# Validate docker-compose.yml
docker-compose config

# Dry run
docker-compose up --dry-run
```

---

## Support

For issues and questions:
- Check logs first: `docker-compose logs -f`
- Verify .env configuration
- Ensure QuestDB is accessible
- Check GitHub repository for updates