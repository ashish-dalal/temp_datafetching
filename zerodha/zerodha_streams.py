"""
Zerodha WebSocket Stream Definitions for Indian Market Data Collection
Reference: https://kite.trade/docs/connect/v3/
"""




STREAM_TYPES = {
    # Full Tick Data
    "full": {
        "mode": "full",
        "table": "ticks_full",
        "description": "Complete tick data including OHLC, volume, order book depth"
    },
    # Quote Data
    "quote": {
        "mode": "quote",
        "table": "ticks_quote",
        "description": "OHLC, volume, last price, and aggregated order book"
    },
    # LTP (Last Traded Price)
    "ltp": {
        "mode": "ltp",
        "table": "ticks_ltp",
        "description": "Only last traded price"
    }
}

# QuestDB Schema Definitions for each stream type
TABLE_SCHEMAS = {
    "ticks_full": """
        CREATE TABLE IF NOT EXISTS ticks_full (
            instrument_token LONG,
            exchange SYMBOL,
            trading_symbol SYMBOL,
            last_price DOUBLE,
            last_quantity LONG,
            average_price DOUBLE,
            volume LONG,
            buy_quantity LONG,
            sell_quantity LONG,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            last_trade_time TIMESTAMP,
            timestamp TIMESTAMP
        ) timestamp(timestamp)
        PARTITION BY DAY;
    """,
    "ticks_quote": """
        CREATE TABLE IF NOT EXISTS ticks_quote (
            instrument_token LONG,
            exchange SYMBOL,
            trading_symbol SYMBOL,
            last_price DOUBLE,
            last_quantity LONG,
            average_price DOUBLE,
            volume LONG,
            buy_quantity LONG,
            sell_quantity LONG,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            last_trade_time TIMESTAMP,
            timestamp TIMESTAMP
        ) timestamp(timestamp)
        PARTITION BY DAY;
    """,
    "ticks_ltp": """
        CREATE TABLE IF NOT EXISTS ticks_ltp (
            instrument_token LONG,
            exchange SYMBOL,
            trading_symbol SYMBOL,
            last_price DOUBLE,
            timestamp TIMESTAMP
        ) timestamp(timestamp)
        PARTITION BY DAY;
    """
}