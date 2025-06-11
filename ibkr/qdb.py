import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any
import os
from pathlib import Path
from dotenv import load_dotenv
from questdb.ingress import Sender, TimestampNanos

# Load environment variables from parent directory
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(ENV_PATH)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuestDBClient:
    """Unified QuestDB client for IBKR data with async operations"""
    
    def __init__(self):
        # Get connection details from environment
        host = os.getenv('QUESTDB_HOST', 'localhost')
        port = os.getenv('QUESTDB_PORT', '443')
        user = os.getenv('QUESTDB_USER', 'admin')
        password = os.getenv('QUESTDB_PASSWORD', 'quest')
        
        self.conf = (
            f"https::addr={host}:{port};"
            f"username={user};"
            f"password={password};"
        )
        self.sender = None
        self.connected = False
        
        logger.info(f"QuestDB config: {host}:{port} (user: {user})")
    
    async def connect(self):
        """Establish connection to QuestDB and create tables"""
        try:
            self.sender = Sender.from_conf(self.conf)
            self.connected = True
            await self.create_tables()
            logger.info("Connected to QuestDB successfully")
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            self.connected = False
            raise
    
    async def create_tables(self):
        """Create required tables for IBKR data"""
        tables = {
            'stocks_ticks': """
                CREATE TABLE IF NOT EXISTS stocks_ticks (
                    symbol SYMBOL CAPACITY 256 CACHE,
                    price DOUBLE,
                    bid DOUBLE,
                    ask DOUBLE,
                    volume LONG,
                    exchange SYMBOL,
                    timestamp TIMESTAMP
                ) timestamp(timestamp)
                PARTITION BY DAY;
            """,
            'indices_ticks': """
                CREATE TABLE IF NOT EXISTS indices_ticks (
                    symbol SYMBOL CAPACITY 256 CACHE,
                    price DOUBLE,
                    bid DOUBLE,
                    ask DOUBLE,
                    volume LONG,
                    exchange SYMBOL,
                    timestamp TIMESTAMP
                ) timestamp(timestamp)
                PARTITION BY DAY;
            """,
            'stocks_candlesticks': """
                CREATE TABLE IF NOT EXISTS stocks_candlesticks (
                    symbol SYMBOL CAPACITY 256 CACHE,
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
                    symbol SYMBOL CAPACITY 256 CACHE,
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
        
        # Note: Table creation via ingress client requires HTTP requests
        # For now, tables should be created manually or via separate script
        logger.info("Table schemas defined - ensure tables exist in QuestDB")
    
    async def insert_batch(self, batch: List[Dict[str, Any]]):
        """Insert a batch of data into appropriate tables"""
        if not self.connected or not self.sender:
            raise Exception("Not connected to QuestDB")
        
        try:
            with Sender.from_conf(self.conf) as sender:
                for data in batch:
                    await self._insert_single_record(sender, data)
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            raise
    
    async def _insert_single_record(self, sender, data: Dict[str, Any]):
        """Insert a single record based on data type"""
        data_type = data.get('data_type')
        
        if data_type == 'tick':
            await self._insert_tick_data(sender, data)
        elif data_type == 'candlestick':
            await self._insert_candlestick_data(sender, data)
        else:
            logger.warning(f"Unknown data type: {data_type}")
    
    async def _insert_tick_data(self, sender, data: Dict[str, Any]):
        """Insert tick data into appropriate table"""
        sec_type = data.get('secType', 'STK')
        table_name = 'stocks_ticks' if sec_type == 'STK' else 'indices_ticks'
        
        # Convert timestamp to nanoseconds
        ts = TimestampNanos.from_datetime(
            datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        )
        
        sender.row(
            table_name,
            symbols={'symbol': str(data['symbol'])},
            columns={
                'price': float(data['price']),
                'bid': float(data.get('bid', 0)),
                'ask': float(data.get('ask', 0)),
                'volume': int(data.get('volume', 0))
            },
            at=ts
        )
    
    async def _insert_candlestick_data(self, sender, data: Dict[str, Any]):
        """Insert candlestick data into appropriate table"""
        sec_type = data.get('secType', 'STK')
        table_name = 'stocks_candlesticks' if sec_type == 'STK' else 'indices_candlesticks'
        
        # Convert timestamp to nanoseconds
        ts = TimestampNanos.from_datetime(
            datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        )
        
        sender.row(
            table_name,
            symbols={
                'symbol': str(data['symbol']),
                'timeframe': data.get('timeframe', '1min')
            },
            columns={
                'open': float(data['open']),
                'high': float(data['high']),
                'low': float(data['low']),
                'close': float(data['close']),
                'volume': int(data.get('volume', 0))
            },
            at=ts
        )
    
    def disconnect(self):
        """Disconnect from QuestDB"""
        if self.sender:
            self.sender = None
        self.connected = False
        logger.info("Disconnected from QuestDB")