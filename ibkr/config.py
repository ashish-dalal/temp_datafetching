import os

# IBKR API configuration
IBKR_HOST = '127.0.0.1'
IBKR_PORT = 7497
IBKR_CLIENT_ID = 0




# QuestDB configuration
QUESTDB_HOST='qdb2.twocc.in'
QUESTDB_PORT=443
QUESTDB_USER='2Cents'
QUESTDB_PASSWORD='2Cents1012cc'

# Asset classes and their contract types
ASSET_CLASSES = {
    'stocks': 'STK',
    'futures': 'FUT',
    'options': 'OPT',
    'indices': 'IND'
}

# Countries and their exchanges
COUNTRIES = {
    'US': {
        'stocks': ['NASDAQ'],
        'futures': ['GLOBEX', 'NYMEX', 'CBOT', 'CME'],
        'options': ['CBOE', 'NASDAQOM'],
        'indices': ['NYSE', 'NASDAQ']
    },
    'India': {
        'stocks': ['NSE'],
        'futures': ['NSE'],
        'options': ['NSE'],
        'indices': ['NSE']
    },
    'Japan': {
        'stocks': ['TSEJ'],
        'futures': ['OSE'],
        'options': ['OSE'],
        'indices': ['TSEJ']
    }
}

# Currency by country
CURRENCIES = {
    'US': 'USD',
    'India': 'INR',
    'Japan': 'JPY'
}

# Timeframes for historical data
TIMEFRAMES = ['1 min', '5 mins', '15 mins', '1 hour', '1 day']

# QuestDB table names (by asset class)
TABLES = {
    'ticks': {
        asset_class: f"{asset_class}_ticks"
        for asset_class in ASSET_CLASSES
    },
    'candlesticks': {
        asset_class: f"{asset_class}_candlesticks"
        for asset_class in ASSET_CLASSES
    }
}

# Market data subscription batch size
BATCH_SIZE = 25

# Historical data batch size
HISTORICAL_BATCH_SIZE = 10

# Duration to collect tick data per batch (seconds)
TICK_COLLECTION_DURATION = 60