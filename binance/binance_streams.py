"""
Binance WebSocket Stream Definitions for Futures Market Data Collection
Reference: https://binance-docs.github.io/apidocs/futures/en/
"""





STREAM_TYPES = {
    # Aggregate Trade Streams
    "aggTrade": {
        "suffix": "@aggTrade",
        "table": "agg_trades",
        "description": "Compressed real-time trades compressed by price level"
    },
    
    # Mark Price Stream
    "markPrice": {
        "suffix": "@markPrice@1s",
        "table": "mark_prices",
        "description": "Mark price and funding rate for all symbols"
    },
    
    # Symbol Mini-Ticker
    "miniTicker": {
        "suffix": "@miniTicker",
        "table": "mini_tickers",
        "description": "24hr rolling window mini-ticker statistics"
    },
    
    # Symbol Ticker
    "ticker": {
        "suffix": "@ticker",
        "table": "tickers",
        "description": "24hr rolling window ticker statistics"
    },
    
    # Book Ticker
    "bookTicker": {
        "suffix": "@bookTicker",
        "table": "book_tickers",
        "description": "Best bid/ask price and quantity"
    },
    
    # Candlestick/Kline
    "kline": {
        "suffix": "@kline_",
        "intervals": ["1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"],  # Reduced intervals for initial testing
        "table": "klines",
        "description": "Candlestick data at various intervals"
    },
    
    # Individual Symbol Book Depth
    "depth": {
        "suffix": "@depth",
        "levels": [5, 10, 20],  # Available depth levels
        "speeds": [100, 250, 500],  # Update speed in ms
        "table": "order_books",
        "description": "Order book depth data"
    },
    
    # Diff. Book Depth
    "depthDiff": {
        "suffix": "@depth@",
        "speeds": [100, 250, 500],  # Update speed in ms
        "table": "order_book_updates",
        "description": "Order book updates (differences)"
    },
    
    # Liquidation Orders
    "forceOrder": {
        "suffix": "@forceOrder",
        "table": "liquidations",
        "description": "Liquidation orders as they happen"
    },
    
    # Partial Book Depth
    "partialBookDepth": {
        "suffix": "@depth",
        "levels": [5, 10, 20],
        "table": "partial_order_books",
        "description": "Top bids and asks at specified depth"
    },
    
    # Composite Index
    "compositeIndex": {
        "suffix": "@compositeIndex",
        "table": "composite_indexes",
        "description": "Composite index information"
    }
}

# QuestDB Schema Definitions for each stream type
TABLE_SCHEMAS = {
    "agg_trades": """
        CREATE TABLE IF NOT EXISTS agg_trades (
            symbol SYMBOL CAPACITY 256 CACHE,
            price DOUBLE,
            quantity DOUBLE,
            first_trade_id LONG,
            last_trade_id LONG,
            transaction_time TIMESTAMP,
            is_buyer_maker BOOLEAN,
            timestamp TIMESTAMP
        ) timestamp(transaction_time)
        PARTITION BY DAY;
    """,
    
    "mark_prices": """
        CREATE TABLE IF NOT EXISTS mark_prices (
            symbol SYMBOL CAPACITY 256 CACHE,
            mark_price DOUBLE,
            funding_rate DOUBLE,
            next_funding_time TIMESTAMP,
            index_price DOUBLE,
            timestamp TIMESTAMP
        ) timestamp(timestamp)
        PARTITION BY DAY;
    """,
    
    "mini_tickers": """
        CREATE TABLE IF NOT EXISTS mini_tickers (
            symbol SYMBOL,
            close_price DOUBLE,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            base_volume DOUBLE,
            quote_volume DOUBLE,
            event_time TIMESTAMP
        ) timestamp(event_time);
    """,
    
    "tickers": """
        CREATE TABLE IF NOT EXISTS tickers (
            symbol SYMBOL,
            price_change DOUBLE,
            price_change_percent DOUBLE,
            weighted_avg_price DOUBLE,
            last_price DOUBLE,
            last_quantity DOUBLE,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            base_volume DOUBLE,
            quote_volume DOUBLE,
            open_time TIMESTAMP,
            close_time TIMESTAMP,
            first_trade_id LONG,
            last_trade_id LONG,
            trade_count LONG,
            event_time TIMESTAMP
        ) timestamp(event_time);
    """,
    
    "book_tickers": """
        CREATE TABLE IF NOT EXISTS book_tickers (
            symbol SYMBOL,
            bid_price DOUBLE,
            bid_qty DOUBLE,
            ask_price DOUBLE,
            ask_qty DOUBLE,
            event_time TIMESTAMP
        ) timestamp(event_time);
    """,
    
    "klines": """
        CREATE TABLE IF NOT EXISTS klines (
            symbol SYMBOL CAPACITY 256 CACHE,
            interval SYMBOL CAPACITY 8 CACHE,
            open_time TIMESTAMP,
            close_time TIMESTAMP,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            quote_volume DOUBLE,
            trades LONG,
            taker_buy_volume DOUBLE,
            taker_buy_quote_volume DOUBLE,
            is_closed BOOLEAN,
            timestamp TIMESTAMP
        ) timestamp(timestamp)
        PARTITION BY DAY;
    """,
    
    "order_books": """
        CREATE TABLE IF NOT EXISTS order_books (
            symbol SYMBOL,
            level INT,
            side STRING,
            price DOUBLE,
            quantity DOUBLE,
            update_time TIMESTAMP
        ) timestamp(update_time);
    """,
    
    "order_book_updates": """
        CREATE TABLE IF NOT EXISTS order_book_updates (
            symbol SYMBOL,
            side STRING,
            price DOUBLE,
            quantity DOUBLE,
            update_id LONG,
            update_time TIMESTAMP
        ) timestamp(update_time);
    """,
    
    "liquidations": """
        CREATE TABLE IF NOT EXISTS liquidations (
            symbol SYMBOL,
            side STRING,
            order_type STRING,
            timeInForce STRING,
            price DOUBLE,
            quantity DOUBLE,
            average_price DOUBLE,
            status STRING,
            event_time TIMESTAMP
        ) timestamp(event_time);
    """,
    
    "composite_indexes": """
        CREATE TABLE IF NOT EXISTS composite_indexes (
            symbol SYMBOL,
            price DOUBLE,
            component_symbols STRING[],
            component_weights DOUBLE[],
            event_time TIMESTAMP
        ) timestamp(event_time);
    """
} 