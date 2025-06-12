# Financial Data Pipeline Orchestrator

A comprehensive process manager for collecting financial market data from Binance, Zerodha, and IBKR exchanges. The orchestrator manages three independent schedulers that handle their own timing and data collection cycles.

## Table of Contents

- [Setup & Installation](#setup--installation)
- [Architecture Overview](#architecture-overview)
- [Configuration](#configuration)
- [Operation & Usage](#operation--usage)
- [Daily Operation Flow](#daily-operation-flow)
- [Troubleshooting](#troubleshooting)
- [TODO](#todo)

## Setup & Installation

### Prerequisites

- Python 3.10 or higher
- QuestDB server running and accessible
- For IBKR: TWS/Gateway running on designated server
- For Zerodha: Valid API credentials and access

### Installation Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/2CentsCapital/datafetching
   cd datafetching
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Create configuration file:**
   
   Copy and customize the `config.yaml` file with your specific settings (see [Configuration](#configuration) section).

4. **Create environment file:**
   
   Create a `.env` file in the root directory with the following variables:
   ```bash
   # Zerodha API Configuration
   ZERODHA_API_KEY=your_zerodha_api_key
   ZERODHA_API_SECRET=your_zerodha_api_secret
   ZERODHA_ACCESS_TOKEN=your_zerodha_access_token

   # QuestDB Configuration
   QUESTDB_HOST=your_questdb_host
   QUESTDB_PORT=443
   QUESTDB_USER=your_questdb_username
   QUESTDB_PASSWORD=your_questdb_password

   # IBKR Configuration
   IBKR_HOST=your_ibkr_gateway_host
   IBKR_PORT=7497
   IBKR_CLIENT_ID=0
   ```

5. **Create logs directory:**
   ```bash
   mkdir -p logs
   ```

6. **Verify setup:**
   ```bash
   # Test individual components if needed
   python ibkr/test_connection.py
   ```

### Running the Orchestrator

Start the orchestrator with:
```bash
python orchestrator.py
```

The orchestrator will:
- Start all three schedulers (Binance, Zerodha, IBKR)
- Monitor process health and restart if needed
- Handle graceful shutdown on interruption
- Log all activities to timestamped files

## Architecture Overview

The orchestrator follows a multi-process architecture where each exchange has its own dedicated scheduler that manages timing and data collection independently.

### Core Components

#### 1. **Main Orchestrator (`orchestrator.py`)**
- **Process Manager**: Manages lifecycle of all scheduler processes
- **Health Monitor**: Monitors memory usage, uptime, and process status
- **Signal Handling**: Graceful shutdown with 2-minute timeout
- **Logging**: Centralized logging with rotation

#### 2. **Exchange Schedulers**

##### **Binance Connector (`binance/run_connector.py`)**
- **Operation**: Continuous 24/7 data collection
- **Data Types**: Mark prices, tickers, aggregated trades, book tickers, klines
- **Tables**: `mark_prices`, `tickers`, `agg_trades`, `book_tickers`, `klines`
- **Symbols**: All available Binance Futures symbols (~600+)
- **Storage**: Real-time insertion to QuestDB

##### **Zerodha Scheduler (`zerodha/scheduler_ost.py`)**
- **Token Renewal**: Daily at 8:00 AM IST
- **Market Hours**: 9:10 AM - 3:35 PM IST (5-minute buffer on each side)
- **Data Types**: Full ticks, quote ticks, LTP ticks
- **Tables**: `ticks_full`, `ticks_quote`, `ticks_ltp`
- **Weekend Handling**: Automatically skips Saturday & Sunday
- **Storage**: Batched insertion to QuestDB

##### **IBKR Scheduler (`ibkr/scheduler_ost.py`)**
- **Market Hours**: 7:20 PM - 1:35 AM IST (US trading hours)
- **Pre-market Check**: 5 minutes before market open for QuestDB table verification
- **Data Types**: Stock ticks, index ticks, candlesticks
- **Tables**: `stocks_ticks`, `indices_ticks`, `stocks_candlesticks`, `indices_candlesticks`
- **Weekend Handling**: Automatically skips Saturday & Sunday
- **Storage**: Batched insertion to QuestDB

#### 3. **Data Flow Architecture**

```
Orchestrator
├── Binance Scheduler (24/7)
│   ├── WebSocket Connections → Queue → QuestDB Writer
│   └── Handles: markPrice, ticker, aggTrade, bookTicker, kline
├── Zerodha Scheduler (Market Hours)
│   ├── Token Renewal → Data Collection → Queue → QuestDB Writer
│   └── Handles: full ticks, quote ticks, LTP ticks
└── IBKR Scheduler (US Market Hours)
    ├── Table Setup → Data Collection → Queue → QuestDB Writer
    └── Handles: stock ticks, index ticks, candlesticks
```

### Process Management Features

- **Health Monitoring**: Memory usage, uptime tracking, restart counting
- **Automatic Restart**: Failed processes are automatically restarted
- **Graceful Shutdown**: 15-second grace period before force termination
- **Resource Monitoring**: Configurable memory limits with warnings
- **Centralized Logging**: All process outputs logged to individual files

## Configuration

### Key Configuration Options

The `config.yaml` file contains the following essential settings:

#### **Timing Configuration**
```yaml
zerodha:
  token_renewal_time: "08:00"    # Daily token renewal
  data_start_time: "09:10"       # Data collection start
  data_end_time: "15:35"         # Data collection end
  market_start_time: "09:15"     # Actual market open
  market_end_time: "15:30"       # Actual market close

ibkr:
  market_start_time: "19:20"     # US market start in IST
  market_end_time: "01:35"       # US market end in IST
  pre_market_check_minutes: 5    # Buffer before market for checks
```

#### **Process Management**
```yaml
timeouts:
  process_shutdown: 15           # Grace period before force kill
  token_validation: 5            # Token verification timeout

retry:
  max_attempts: 5                # Max retry attempts for critical operations
  urgent_alert_interval: 5       # Alert frequency for failures

health:
  check_interval: 30             # Health check frequency (seconds)
  memory_limit_mb: 2048          # Memory limit per process
```

### Complete Configuration Reference

#### **File Paths**
```yaml
paths:
  binance_script: "binance/run_connector.py"
  zerodha_script: "zerodha/scheduler_ost.py"
  ibkr_script: "ibkr/scheduler_ost.py"
  ibkr_create_tables_script: "ibkr/create_tables.py"
```

#### **Logging Settings**
```yaml
logging:
  level: "DEBUG"                 # Log level: DEBUG, INFO, WARNING, ERROR
  max_file_size: 104857600      # Maximum log file size (100MB)
```

#### **Advanced Settings**
```yaml
scheduling:
  renewal_cycle: 86355          # Seconds between token renewals (23h 59m 15s)

ibkr:
  connection_retry_delay: 5     # Seconds between connection retries
  max_retries: 2                # Maximum connection attempts
```

## Operation & Usage

### Starting the System

1. **Ensure prerequisites are running:**
   - QuestDB server is accessible
   - IBKR Gateway/TWS is running (for IBKR data)
   - Network connectivity to exchanges

2. **Start the orchestrator:**
   ```bash
   python orchestrator.py
   ```

3. **Monitor startup logs:**
   ```bash
   tail -f logs/orchestrator_*.log
   ```

### Monitoring Operations

#### **Check Process Status**
The orchestrator logs process health every 30 seconds:
```
2025-01-15 10:30:00 - ProcessManager.binance - INFO - Process binance: PID=1234, Memory=245.1MB, Uptime=3600.0s, Restarts=1
```

#### **Individual Process Logs**
Each scheduler writes to its own log file:
```bash
tail -f logs/binance_output.log     # Binance data collection
tail -f logs/zerodha_output.log     # Zerodha scheduler
tail -f logs/ibkr_output.log        # IBKR scheduler
```

#### **QuestDB Data Verification**
Check data insertion success:
```sql
-- In QuestDB console
SELECT count(*) FROM tickers WHERE timestamp > dateadd('h', -1, now());  -- Binance
SELECT count(*) FROM ticks_full WHERE timestamp > dateadd('h', -1, now()); -- Zerodha
SELECT count(*) FROM stocks_ticks WHERE timestamp > dateadd('h', -1, now()); -- IBKR
```

### Stopping the System

#### **Graceful Shutdown**
```bash
# Send SIGTERM or SIGINT (Ctrl+C)
kill -TERM <orchestrator_pid>
```

#### **Emergency Stop**
```bash
# Force kill if graceful shutdown fails
kill -KILL <orchestrator_pid>
```

## Daily Operation Flow

Understanding the typical daily cycle helps with monitoring and troubleshooting:

### **Pre-Market Phase (00:00 - 08:00 IST)**
- **Binance**: Continuous data collection ongoing
- **Zerodha**: Scheduler sleeping until token renewal time
- **IBKR**: Post-market phase, sleeping until next pre-market check

### **Zerodha Token Renewal (08:00 IST)**
- **08:00 AM**: Zerodha scheduler wakes up
- **08:00 - 08:05**: Token renewal process (browser automation)
- **08:05 - 09:10**: Waiting for market data collection time
- **Binance**: Continuous operation
- **IBKR**: Still sleeping

### **Indian Market Hours (09:10 - 15:35 IST)**
- **09:10 AM**: Zerodha data collection begins
- **09:15 AM**: Indian market officially opens
- **15:30 PM**: Indian market officially closes
- **15:35 PM**: Zerodha data collection ends, scheduler sleeps
- **Binance**: Continuous operation
- **IBKR**: Still sleeping

### **IBKR Pre-Market Check (19:15 IST)**
- **19:15 PM**: IBKR scheduler wakes up for pre-market checks
- **19:15 - 19:20**: QuestDB connection and table verification
- **Binance**: Continuous operation
- **Zerodha**: Sleeping until next day

### **US Market Hours (19:20 - 01:35 IST)**
- **19:20 PM**: IBKR data collection begins (US market open)
- **01:35 AM**: IBKR data collection ends (US market close)
- **01:35 - 19:15**: IBKR scheduler sleeps
- **Binance**: Continuous operation throughout

### **Weekend Behavior**
- **Binance**: Continues 24/7 (cryptocurrency markets don't close)
- **Zerodha**: Automatically skips weekends, resumes Monday morning
- **IBKR**: Automatically skips weekends, resumes Monday evening

## Troubleshooting

### **Configuration Issues**

#### **Missing .env File**
```bash
# Error: Missing QuestDB configuration parameters: QUESTDB_HOST, QUESTDB_PORT
# Solution: Create .env file with required variables (see Installation section)
```

#### **Wrong Script Paths**
```bash
# Error: Failed to start process zerodha: [Errno 2] No such file or directory
# Solution: Verify paths in config.yaml point to correct scheduler files
```

#### **Invalid Configuration Values**
```bash
# Error: Failed to load config from config.yaml
# Solution: Check YAML syntax and ensure all required sections exist
```

### **Zerodha Token Renewal Issues**

#### **TOTP Automation Failures**
- **Symptom**: Token renewal fails during browser automation
- **Cause**: Playwright timeouts, website changes, or TOTP timing issues
- **Current Status**: Known issue with bypass in place
- **Workaround**: Manual token generation if automation fails repeatedly

#### **Listener Port Conflicts**
- **Symptom**: Token renewal fails to start listener
- **Cause**: Port 8080 already in use
- **Solution**: Kill conflicting process or change port in listener configuration

### **QuestDB Connection Issues**

#### **Connection Failures**
```bash
# Error: Failed to connect to QuestDB: Connection refused
# Check: QuestDB server status and network connectivity
# Solution: Verify QUESTDB_HOST and QUESTDB_PORT in .env
```

#### **Authentication Errors**
```bash
# Error: HTTP 401 Unauthorized
# Solution: Verify QUESTDB_USER and QUESTDB_PASSWORD in .env
```

#### **Table Creation Issues**
```bash
# Error: Tables missing or creation failed
# Solution: Check QuestDB permissions and manual table creation if needed
```

### **IBKR Gateway Issues**

#### **Connection Failures**
```bash
# Error: IBKR connection failed after all retries
# Check: TWS/Gateway running and accepting connections
# Solution: Verify IBKR_HOST, IBKR_PORT, and IBKR_CLIENT_ID
```

#### **Multiple Client Conflicts**
- **Symptom**: Connection refused or unexpected disconnections
- **Cause**: IBKR limits concurrent API connections
- **Solution**: Ensure unique IBKR_CLIENT_ID and close other API connections

### **Process Management Issues**

#### **High Memory Usage**
```bash
# Warning: Process binance using 2100.0MB (limit: 2048MB)
# Solution: Increase memory_limit_mb in config or investigate memory leaks
```

#### **Frequent Restarts**
- **Symptom**: High restart counts in health monitoring
- **Investigation**: Check individual process logs for error patterns
- **Solution**: Address underlying issues (network, credentials, etc.)

#### **Shutdown Timeout**
```bash
# Error: Shutdown timeout reached! Force killing all processes...
# Cause: Processes not responding to graceful shutdown
# Solution: Increase process_shutdown timeout or investigate hanging processes
```

### **Data Quality Issues**

#### **No Data in QuestDB**
1. **Check process status**: Ensure schedulers are running
2. **Verify connections**: Test QuestDB and exchange connectivity
3. **Review logs**: Look for insertion errors or queue issues
4. **Timing**: Ensure data collection is within market hours

#### **Incomplete Data**
1. **Queue overflow**: Check for queue size warnings in logs
2. **Network issues**: Look for connection errors or timeouts
3. **Rate limiting**: Check for API rate limit messages

## TODO

### **Planned Improvements**

1. **Zerodha Token Automation Fix**
   - Resolve Playwright automation reliability issues
   - Remove current bypass workaround
   - Implement backup token generation methods

2. **Enhanced Monitoring**
   - Web dashboard for real-time monitoring
   - Email/SMS alerts for critical failures
   - Data quality metrics and reporting

3. **Configuration Improvements**
   - Environment-specific configuration files
   - Runtime configuration updates without restart
   - Configuration validation and testing tools

4. **Robustness Enhancements**
   - Improved error recovery mechanisms
   - Circuit breakers for external dependencies
   - Data integrity verification and repair

5. **Performance Optimization**
   - Memory usage optimization
   - Batch processing improvements
   - Connection pooling for QuestDB

### **Future Features**

1. **Additional Data Sources**
   - Support for more exchanges and asset classes
   - Alternative data providers integration

2. **Advanced Scheduling**
   - Holiday calendar integration
   - Dynamic schedule adjustments
   - Market hours auto-detection

3. **Data Processing Pipeline**
   - Real-time data transformation
   - Anomaly detection and alerting
   - Data aggregation and analytics

---

## Support

For issues and questions:
- Check logs first: `tail -f logs/orchestrator_*.log`
- Verify configuration: Ensure .env and config.yaml are correct
- Test connections: Use individual test scripts
- Monitor resources: Check memory usage and disk space# Financial Data Pipeline Orchestrator