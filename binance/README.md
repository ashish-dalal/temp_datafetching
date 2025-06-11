# Binance Futures Data Fetcher

A high-performance data collection tool for Binance Futures market data. This tool can fetch real-time data for all 600+ trading pairs on Binance Futures.

## Features

- Fetch data from **all 600+ Binance Futures symbols** in real-time
- Supports multiple data stream types:
  - Mark Price / Funding Rate
  - 24h Ticker Statistics
  - Aggregated Trades
  - Order Book Best Bid/Ask
  - Candlestick Data (Klines)
- Automatic batch processing to handle all symbols efficiently
- Database storage with QuestDB for time-series analysis
- Configurable batch sizes and timeouts

## Installation

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Make sure you have QuestDB installed and running (optional if you only want to view data)

## Usage

### Basic Usage

To run with default settings (5 major symbols):

```bash
python run_connector.py
```

### Fetch All Symbols

To fetch data for all available Binance Futures symbols:

```bash
python run_connector.py --all-symbols
```

### Fetch Specific Symbols

To fetch data for specific symbols:

```bash
python run_connector.py --symbols btcusdt,ethusdt,solusdt,adausdt
```

### Select Specific Stream Types

You can choose which types of data streams to receive:

```bash
# Only fetch mark price and kline data
python run_connector.py --all-symbols --mark-price --klines

# Only fetch order book and trade data
python run_connector.py --all-symbols --book-ticker --agg-trades
```

### Multiple Timeframes

For candlestick data (klines), you can specify multiple timeframes:

```bash
# Fetch 1-minute, 5-minute and 1-hour candles
python run_connector.py --all-symbols --klines --kline-intervals 1m,5m,1h

# Available intervals: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
```

### Performance Options

Control batch sizes to balance performance:

```bash
# Set maximum 50 symbols per WebSocket connection (default: 25)
python run_connector.py --all-symbols --batch-size 50

# Set database batch size to 200 records (default: 100)
python run_connector.py --all-symbols --db-batch-size 200

# Set batch timeout to 1 second (default: 0.5)
python run_connector.py --all-symbols --batch-timeout 1.0
```

### Reduce Output

Disable console printing for better performance:

```bash
python run_connector.py --all-symbols --disable-prints
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `-a, --all-symbols` | Fetch all available Binance Futures symbols |
| `-s, --symbols SYMBOLS` | Comma-separated list of symbols to fetch |
| `--mark-price` | Fetch mark price data |
| `--ticker` | Fetch 24hr ticker data |
| `--agg-trades` | Fetch aggregated trades data |
| `--book-ticker` | Fetch best bid/ask data |
| `--klines` | Fetch candlestick data |
| `--kline-intervals INTERVALS` | Comma-separated list of kline intervals (e.g., 1m,5m,1h) |
| `-b, --batch-size SIZE` | Maximum number of symbols per WebSocket connection (default: 25) |
| `--db-batch-size SIZE` | Batch size for database writes (default: 100) |
| `-t, --batch-timeout SECONDS` | Timeout in seconds between batch writes (default: 0.5) |
| `-d, --disable-prints` | Disable printing data to console |

## Requirements

- Python 3.8+
- aiohttp
- websockets
- questdb
- python-dotenv 