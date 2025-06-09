import logging
import time
import random
import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from questdb.ingress import Sender, TimestampNanos
from typing import Dict, List, Any, Optional, Union

ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(ENV_PATH)
QDB_HOST = os.getenv('QUESTDB_HOST')
QDB_PORT = os.getenv('QUESTDB_PORT')
QDB_USER = os.getenv('QUESTDB_USER')
QDB_PASSWORD = os.getenv('QUESTDB_PASSWORD')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuestDBWriter:
    """A class to handle writing data to QuestDB."""
    
    def __init__(self, questdb_url: str, username: str = None, password: str = None): # type:ignore
        """Initialize the QuestDBWriter with the QuestDB REST API URL and optional auth credentials."""
        self.questdb_url = questdb_url
        self.auth = None
        if username and password:
            self.auth = (username, password)
            logger.info(f"Using authentication with user: {username}")
            
        self.buffer = []
        self.candlestick_buffer = []
        self.buffer_size_limit = 100  # Maximum number of records to buffer before flushing
        self.last_flush_time = time.time()
        self.flush_interval = 2.0  # Flush at least every 2 seconds
        self.table_cache = {}  # Cache table existence to avoid repeated checks
        
        # Test connection
        self._test_connection()
        logger.info("Connected to QuestDB")
        
    def _test_connection(self) -> None:
        """Test the connection to QuestDB."""
        try:
            # Try a simple query to test connection
            response = requests.get(f"{self.questdb_url}/exec", 
                                   params={"query": "SELECT 1"}, 
                                   auth=self.auth,
                                   timeout=5)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB at {self.questdb_url}: {e}")
            raise ConnectionError(f"Could not connect to QuestDB: {e}")
    
    def insert_tick(self, tick_data: Dict[str, Any]) -> None:
        """Add a tick data record to the buffer."""
        if not tick_data:
            return
            
        # Add the data to the buffer
        self.buffer.append(tick_data)
        
        # Check if we should flush the buffer
        current_time = time.time()
        if (len(self.buffer) >= self.buffer_size_limit or 
            current_time - self.last_flush_time >= self.flush_interval):
            self.flush()
    
    def insert_candlestick(self, candlestick_data: Dict[str, Any]) -> None:
        """Add a candlestick data record to the buffer."""
        if not candlestick_data:
            return
            
        # Add the data to the candlestick buffer
        self.candlestick_buffer.append(candlestick_data)
        
        # Check if we should flush the buffer
        current_time = time.time()
        if (len(self.candlestick_buffer) >= self.buffer_size_limit or 
            current_time - self.last_flush_time >= self.flush_interval):
            self.flush()
    
    def flush(self) -> int:
        """Flush the buffers to QuestDB and return the number of records flushed."""
        records_flushed = 0
        
        # Flush tick data
        if self.buffer:
            # Group ticks by security type
            stock_ticks = []
            index_ticks = []
            
            for tick in self.buffer:
                sec_type = tick.get('secType', '').upper()
                if sec_type == 'STK':
                    stock_ticks.append(tick)
                elif sec_type == 'IND':
                    index_ticks.append(tick)
            
            # Process each group
            if stock_ticks:
                records_flushed += self._write_ticks_to_questdb('stocks_ticks', stock_ticks)
                
            if index_ticks:
                records_flushed += self._write_ticks_to_questdb('indices_ticks', index_ticks)
            
            # Clear the tick buffer
            self.buffer = []
        
        # Flush candlestick data
        if self.candlestick_buffer:
            # Group candlesticks by security type
            stock_candlesticks = []
            index_candlesticks = []
            
            for candle in self.candlestick_buffer:
                sec_type = candle.get('secType', '').upper()
                if sec_type == 'STK':
                    stock_candlesticks.append(candle)
                elif sec_type == 'IND':
                    index_candlesticks.append(candle)
            
            # Process each group
            if stock_candlesticks:
                records_flushed += self._write_candlesticks_to_questdb('stocks_candlesticks', stock_candlesticks)
                
            if index_candlesticks:
                records_flushed += self._write_candlesticks_to_questdb('indices_candlesticks', index_candlesticks)
            
            # Clear the candlestick buffer
            self.candlestick_buffer = []
        
        self.last_flush_time = time.time()
        return records_flushed
    
    def _write_ticks_to_questdb(self, table_name: str, ticks: List[Dict[str, Any]]) -> int:
        """Write a batch of ticks to a specific QuestDB table."""
        if not ticks:
            return 0
            
        # Format data for QuestDB ILAP REST API
        influx_lines = []
        
        for tick in ticks:
            # Get required fields
            symbol = tick.get('symbol')
            if not symbol:
                logger.warning(f"Skipping tick with missing symbol: {tick}")
                continue
                
            # Get timestamp or use current time
            timestamp = tick.get('timestamp')
            if isinstance(timestamp, str):
                try:
                    # Convert ISO string to timestamp if needed
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    timestamp = int(dt.timestamp() * 1000000)  # Microseconds
                except ValueError:
                    logger.warning(f"Invalid timestamp format: {timestamp}")
                    timestamp = int(time.time() * 1000000)
            elif not timestamp:
                timestamp = int(time.time() * 1000000)
            
            # Convert timestamp to QuestDB format (nanoseconds since epoch)
            timestamp_ns = int(timestamp * 1000)  # Convert microseconds to nanoseconds
                
            # Prepare tags and fields
            tags = f"symbol={symbol}"
            
            # Build fields string
            fields = []
            
            # Add price fields if available
            if 'price' in tick and tick['price'] is not None:
                fields.append(f"price={float(tick['price'])}")
            if 'bid' in tick and tick['bid'] is not None:
                fields.append(f"bid={float(tick['bid'])}")
            if 'ask' in tick and tick['ask'] is not None:
                fields.append(f"ask={float(tick['ask'])}")
            if 'volume' in tick and tick['volume'] is not None:
                fields.append(f"volume={int(tick['volume'])}")
                
            # Skip if no fields
            if not fields:
                continue
                
            # Build the line
            line = f"{table_name},{tags} {','.join(fields)} {timestamp_ns}"
            influx_lines.append(line)
        
        # Skip if no valid lines
        if not influx_lines:
            return 0
            
        # Send to QuestDB
        try:
            data = '\n'.join(influx_lines)
            response = requests.post(
                f"{self.questdb_url}/write",
                data=data,
                headers={'Content-Type': 'text/plain'},
                auth=self.auth,
                timeout=5
            )
            
            if response.status_code != 204:
                logger.error(f"Failed to write to QuestDB: {response.status_code} - {response.text}")
                return 0
                
            logger.info(f"Successfully flushed {len(influx_lines)} tick rows to QuestDB")
            return len(influx_lines)
            
        except Exception as e:
            logger.error(f"Error writing to QuestDB: {e}")
            return 0
    
    def _write_candlesticks_to_questdb(self, table_name: str, candlesticks: List[Dict[str, Any]]) -> int:
        """Write a batch of candlesticks to a specific QuestDB table."""
        if not candlesticks:
            return 0
            
        # Format data for QuestDB ILAP REST API
        influx_lines = []
        
        for candle in candlesticks:
            # Get required fields
            symbol = candle.get('symbol')
            timeframe = candle.get('timeframe')
            if not symbol or not timeframe:
                logger.warning(f"Skipping candlestick with missing symbol or timeframe: {candle}")
                continue
                
            # Get timestamp or use current time
            timestamp = candle.get('timestamp')
            if isinstance(timestamp, str):
                try:
                    # Convert ISO string to timestamp if needed
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    timestamp = int(dt.timestamp() * 1000000)  # Microseconds
                except ValueError:
                    logger.warning(f"Invalid timestamp format: {timestamp}")
                    timestamp = int(time.time() * 1000000)
            elif not timestamp:
                timestamp = int(time.time() * 1000000)
            
            # Convert timestamp to QuestDB format (nanoseconds since epoch)
            timestamp_ns = int(timestamp * 1000)  # Convert microseconds to nanoseconds
                
            # Prepare tags and fields
            tags = f"symbol={symbol},timeframe={timeframe}"
            
            # Build fields string
            fields = []
            
            # Add OHLCV fields if available
            if 'open' in candle and candle['open'] is not None:
                fields.append(f"open={float(candle['open'])}")
            if 'high' in candle and candle['high'] is not None:
                fields.append(f"high={float(candle['high'])}")
            if 'low' in candle and candle['low'] is not None:
                fields.append(f"low={float(candle['low'])}")
            if 'close' in candle and candle['close'] is not None:
                fields.append(f"close={float(candle['close'])}")
            if 'volume' in candle and candle['volume'] is not None:
                fields.append(f"volume={int(candle['volume'])}")
                
            # Skip if no fields
            if not fields:
                continue
                
            # Build the line
            line = f"{table_name},{tags} {','.join(fields)} {timestamp_ns}"
            influx_lines.append(line)
        
        # Skip if no valid lines
        if not influx_lines:
            return 0
            
        # Send to QuestDB
        try:
            data = '\n'.join(influx_lines)
            response = requests.post(
                f"{self.questdb_url}/write",
                data=data,
                headers={'Content-Type': 'text/plain'},
                auth=self.auth,
                timeout=5
            )
            
            if response.status_code != 204:
                logger.error(f"Failed to write to QuestDB: {response.status_code} - {response.text}")
                return 0
                
            logger.info(f"Successfully flushed {len(influx_lines)} candlestick rows to QuestDB")
            return len(influx_lines)
            
        except Exception as e:
            logger.error(f"Error writing to QuestDB: {e}")
            return 0
    
    def close(self) -> None:
        """Flush any remaining data and close the connection."""
        self.flush()

def get_questdb_writer(questdb_url=None, username=None, password=None):
    """Factory function to get a QuestDBWriter instance"""
    if not questdb_url:
        questdb_url = f"http://{QDB_HOST}:{QDB_PORT}"
    
    # Use config values if not provided
    if not username and QDB_USER:
        username = QDB_USER
    if not password and QDB_PASSWORD:
        password = QDB_PASSWORD
    
    try:
        return QuestDBWriter(questdb_url, username, password) # type:ignore
    except Exception as e:
        logger.error(f"Failed to create QuestDBWriter: {e}")
        raise Exception(f"Could not connect to QuestDB: {e}")