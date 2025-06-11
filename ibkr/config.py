import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from parent directory
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(ENV_PATH)

# IBKR API configuration from environment
IBKR_HOST = os.getenv('IBKR_HOST', '127.0.0.1')
IBKR_PORT = int(os.getenv('IBKR_PORT', 7497))
IBKR_CLIENT_ID = int(os.getenv('IBKR_CLIENT_ID', 0))

# QuestDB configuration from environment
QDB_HOST = os.getenv('QUESTDB_HOST', 'localhost')
QDB_PORT = os.getenv('QUESTDB_PORT', '443')
QDB_USER = os.getenv('QUESTDB_USER', 'admin')
QDB_PASSWORD = os.getenv('QUESTDB_PASSWORD', 'quest')

# Asset classes and their contract types
ASSET_CLASSES = {
    'stocks': 'STK',
    'futures': 'FUT',
    'options': 'OPT',
    'indices': 'IND'
}

# Supported exchanges
EXCHANGES = {
    'US': {
        'stocks': ['SMART', 'NYSE', 'NASDAQ'],
        'indices': ['SMART', 'NYSE', 'NASDAQ']
    }
}

# Currency mappings
CURRENCIES = {
    'US': 'USD',
    'Europe': 'EUR',
    'UK': 'GBP'
}

# Timeframes for candlestick aggregation
TIMEFRAMES = {
    '1min': 60,
    '5min': 300,
    '15min': 900,
    '1hour': 3600,
    '4hour': 14400,
    '1day': 86400
}

# QuestDB table schemas
TABLE_SCHEMAS = {
    'stocks_ticks': """
        CREATE TABLE IF NOT EXISTS stocks_ticks (
            symbol SYMBOL CAPACITY 32768 CACHE,
            price DOUBLE,
            bid DOUBLE,
            ask DOUBLE,
            volume LONG,
            timestamp TIMESTAMP
        ) timestamp(timestamp)
        PARTITION BY DAY;
    """,
    'indices_ticks': """
        CREATE TABLE IF NOT EXISTS indices_ticks (
            symbol SYMBOL CAPACITY 32768 CACHE,
            price DOUBLE,
            bid DOUBLE,
            ask DOUBLE,
            volume LONG,
            timestamp TIMESTAMP
        ) timestamp(timestamp)
        PARTITION BY DAY;
    """,
    'stocks_candlesticks': """
        CREATE TABLE IF NOT EXISTS stocks_candlesticks (
            symbol SYMBOL CAPACITY 32768 CACHE,
            timeframe SYMBOL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume LONG,
            timestamp TIMESTAMP
        ) timestamp(timestamp)
        PARTITION BY MONTH;
    """,
    'indices_candlesticks': """
        CREATE TABLE IF NOT EXISTS indices_candlesticks (
            symbol SYMBOL CAPACITY 32768 CACHE,
            timeframe SYMBOL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume LONG,
            timestamp TIMESTAMP
        ) timestamp(timestamp)
        PARTITION BY MONTH;
    """
}

# Performance settings
QUEUE_MAX_SIZE = 10000
BATCH_SIZE = 100
BATCH_TIMEOUT = 2.0
METRICS_UPDATE_INTERVAL = 10.0

# Logging configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'