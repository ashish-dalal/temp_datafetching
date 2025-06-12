import asyncio
import json
import logging
from datetime import datetime
from typing import List
from questdb.ingress import Sender, TimestampNanos
import os
import sys
from dotenv import load_dotenv
import time
from utils import update_metrics, check_metrics_file
from pathlib import Path



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'

class QuestDBClient:
    def __init__(self):
        # Load environment variables
        load_dotenv(ENV_PATH)
        
        # Get connection details with better error handling
        self.host = os.getenv('QUESTDB_HOST')
        self.port = os.getenv('QUESTDB_PORT')
        self.user = os.getenv('QUESTDB_USER')
        self.password = os.getenv('QUESTDB_PASSWORD')
        
        # Verify we have all required connection parameters
        missing = []
        if not self.host:
            missing.append("QUESTDB_HOST")
        if not self.port:
            missing.append("QUESTDB_PORT")
        if not self.user:
            missing.append("QUESTDB_USER")
        if not self.password:
            missing.append("QUESTDB_PASSWORD")
            
        if missing:
            error_msg = f"Missing QuestDB configuration parameters: {', '.join(missing)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        # Construct the connection string
        self.conf = (
            f"https::addr={self.host}:{self.port};"
            f"username={self.user};"
            f"password={self.password};"
        )
        
        logger.info(f"QuestDB client initialized for {self.host}:{self.port}")
        self.sender = None
        self.connected = False
        
    async def connect(self):
        """Establish a persistent connection to QuestDB"""
        try:
            logger.info(f"Connecting to QuestDB at {self.host}:{self.port}...")
            self.sender = Sender.from_conf(self.conf)
            self.connected = True
            logger.info("Connected to QuestDB")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            logger.error(f"Connection string: https::addr={self.host}:{self.port};username={self.user};password=***")
            self.connected = False
            return False

    # qdb.py
    # qdb.py
    def insert_full_ticks_batch(self, events: List[dict]):
        """Insert a batch of full tick events into QuestDB"""
        if not self.connected or self.sender is None:
            logger.error("Not connected to QuestDB, cannot insert data")
            raise Exception("Not connected to QuestDB")

        if not events:
            logger.debug("No events to insert")
            return

        logger.info(f"Inserting batch of {len(events)} full tick events into QuestDB")

        try:
            success_count = 0
            error_count = 0

            with Sender.from_conf(self.conf) as sender:
                for data in events:
                    try:
                        # Log sample data for debugging, ensuring datetime objects are converted
                        if success_count == 0:
                            log_data = data.copy()
                            if 'last_trade_time' in log_data and isinstance(log_data['last_trade_time'], datetime):
                                log_data['last_trade_time'] = log_data['last_trade_time'].isoformat()
                            if 'exchange_timestamp' in log_data and isinstance(log_data['exchange_timestamp'], datetime):
                                log_data['exchange_timestamp'] = log_data['exchange_timestamp'].isoformat()
                            logger.info(f"Sample tick data: {json.dumps(log_data)[:200]}...")

                        # Ensure timestamp is in nanoseconds
                        ts = TimestampNanos(int(data.get("timestamp", int(time.time() * 1_000_000_000))))

                        # Get symbol fields
                        exchange = data.get('exchange', 'UNKNOWN')
                        trading_symbol = data.get('trading_symbol', data.get('tradingsymbol', 'UNKNOWN'))

                        # Get required numeric fields with fallbacks
                        instrument_token = int(data.get('instrument_token', 0))
                        last_price = float(data.get('last_price', 0.0))
                        last_quantity = int(data.get('last_traded_quantity', 0))
                        average_price = float(data.get('average_traded_price', 0.0))
                        volume = int(data.get('volume_traded', 0))
                        buy_quantity = int(data.get('total_buy_quantity', 0))
                        sell_quantity = int(data.get('total_sell_quantity', 0))

                        # Get OHLC data safely
                        ohlc = data.get('ohlc', {})
                        if not isinstance(ohlc, dict):
                            ohlc = {}
                        open_price = float(ohlc.get('open', 0.0))
                        high_price = float(ohlc.get('high', 0.0))
                        low_price = float(ohlc.get('low', 0.0))
                        close_price = float(ohlc.get('close', 0.0))

                        # Convert last_trade_time to nanoseconds
                        last_trade_time = data.get('last_trade_time')
                        if isinstance(last_trade_time, datetime):
                            last_trade_time = int(last_trade_time.timestamp() * 1_000_000_000)
                        else:
                            last_trade_time = int(last_trade_time or 0)

                        # Insert the row
                        sender.row(
                            'ticks_full',
                            symbols={
                                'exchange': exchange,
                                'trading_symbol': trading_symbol
                            },
                            columns={
                                'instrument_token': instrument_token,
                                'last_price': last_price,
                                'last_quantity': last_quantity,
                                'average_price': average_price,
                                'volume': volume,
                                'buy_quantity': buy_quantity,
                                'sell_quantity': sell_quantity,
                                'open': open_price,
                                'high': high_price,
                                'low': low_price,
                                'close': close_price,
                                'last_trade_time': last_trade_time
                            },
                            at=ts
                        )
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error inserting full tick event: {e}, data: {str(data)[:100]}...")

                # Flush the data
                try:
                    sender.flush()
                    logger.info(f"Successfully flushed {success_count} events to QuestDB (errors: {error_count})")
                except Exception as e:
                    logger.error(f"Error flushing data to QuestDB: {e}")

            logger.info(f"Successfully inserted {success_count}/{len(events)} full tick events into QuestDB")
            return success_count
        except Exception as e:
            logger.error(f"Error inserting full ticks batch: {e}")
            self.connected = False
            raise

    def insert_quote_ticks_batch(self, events: List[dict]):
        """Insert a batch of quote tick events into QuestDB"""
        if not self.connected or self.sender is None:
            raise Exception("Not connected to QuestDB")
        try:
            success_count = 0
            error_count = 0
            with Sender.from_conf(self.conf) as sender:
                for data in events:
                    try:
                        # Get timestamp for this record
                        ts = TimestampNanos(int(data["timestamp"]))
                        
                        # Get symbol fields
                        exchange = data.get('exchange', 'UNKNOWN')
                        trading_symbol = data.get('trading_symbol', data.get('tradingsymbol', 'UNKNOWN'))
                        
                        # Get required numeric fields with fallbacks 
                        # Field names might be different in quote vs full mode
                        instrument_token = int(data.get('instrument_token', 0))
                        last_price = float(data.get('last_price', 0.0))
                        last_quantity = int(data.get('last_quantity', data.get('last_traded_quantity', 0)))
                        average_price = float(data.get('average_price', data.get('average_traded_price', 0.0)))
                        volume = int(data.get('volume', data.get('volume_traded', 0)))
                        buy_quantity = int(data.get('buy_quantity', data.get('total_buy_quantity', 0)))
                        sell_quantity = int(data.get('sell_quantity', data.get('total_sell_quantity', 0)))
                        
                        # Get OHLC data safely
                        ohlc = data.get('ohlc', {})
                        if not isinstance(ohlc, dict):
                            ohlc = {}
                        open_price = float(ohlc.get('open', 0.0))
                        high_price = float(ohlc.get('high', 0.0))
                        low_price = float(ohlc.get('low', 0.0))
                        close_price = float(ohlc.get('close', 0.0))
                        
                        # Convert last_trade_time to nanoseconds
                        last_trade_time = data.get('last_trade_time')
                        if isinstance(last_trade_time, datetime):
                            last_trade_time = int(last_trade_time.timestamp() * 1_000_000_000)
                        else:
                            last_trade_time = int(last_trade_time or 0)
                        
                        # Insert the row
                        sender.row(
                            'ticks_quote',
                            symbols={
                                'exchange': exchange,
                                'trading_symbol': trading_symbol
                            },
                            columns={
                                'instrument_token': instrument_token,
                                'last_price': last_price,
                                'last_quantity': last_quantity,
                                'average_price': average_price,
                                'volume': volume,
                                'buy_quantity': buy_quantity,
                                'sell_quantity': sell_quantity,
                                'open': open_price,
                                'high': high_price,
                                'low': low_price,
                                'close': close_price,
                                'last_trade_time': last_trade_time
                            },
                            at=ts
                        )
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error inserting quote tick event: {e}, data: {str(data)[:100]}...")
                        
                # Flush all data
                try:
                    sender.flush()
                    logger.info(f"Successfully flushed {success_count} events to QuestDB (errors: {error_count})")
                except Exception as e:
                    logger.error(f"Error flushing data to QuestDB: {e}")
                
            logger.info(f"Successfully inserted {success_count}/{len(events)} quote tick events into QuestDB")
            return success_count
        except Exception as e:
            logger.error(f"Error inserting quote ticks batch: {e}")
            self.connected = False
            raise

    def insert_ltp_ticks_batch(self, events: List[dict]):
        """Insert a batch of LTP tick events into QuestDB"""
        if not self.connected or self.sender is None:
            raise Exception("Not connected to QuestDB")
        try:
            success_count = 0
            error_count = 0
            with Sender.from_conf(self.conf) as sender:
                for data in events:
                    try:
                        # Get timestamp for this record
                        ts = TimestampNanos(int(data["timestamp"]))
                        
                        # Get symbol fields
                        exchange = data.get('exchange', 'UNKNOWN')
                        trading_symbol = data.get('trading_symbol', data.get('tradingsymbol', 'UNKNOWN'))
                        
                        # Get required numeric fields
                        instrument_token = int(data.get('instrument_token', 0))
                        last_price = float(data.get('last_price', 0.0))
                        
                        # Insert the row
                        sender.row(
                            'ticks_ltp',
                            symbols={
                                'exchange': exchange,
                                'trading_symbol': trading_symbol
                            },
                            columns={
                                'instrument_token': instrument_token,
                                'last_price': last_price
                            },
                            at=ts
                        )
                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Error inserting LTP tick event: {e}, data: {str(data)[:100]}...")
                
                # Flush all data
                try:
                    sender.flush()
                    logger.info(f"Successfully flushed {success_count} events to QuestDB (errors: {error_count})")
                except Exception as e:
                    logger.error(f"Error flushing data to QuestDB: {e}")
                
            logger.info(f"Successfully inserted {success_count}/{len(events)} LTP tick events into QuestDB")
            return success_count
        except Exception as e:
            logger.error(f"Error inserting LTP ticks batch: {e}")
            self.connected = False
            raise

    async def close(self):
        """Close the persistent connection to QuestDB"""
        if self.sender and self.connected:
            try:
                self.sender.flush()
                self.sender.close()
                self.connected = False
                logger.info("Closed QuestDB connection")
            except Exception as e:
                logger.error(f"Error closing QuestDB connection: {e}")

async def db_writer(queue: asyncio.Queue, qdb_client: QuestDBClient, batch_size: int = 100, batch_timeout: float = 1.0):
    """
    Process data from queue and write to QuestDB
    Args:
        queue: Asyncio queue with data events
        qdb_client: QuestDB client instance
        batch_size: Max number of events to process in one batch
        batch_timeout: Max seconds to wait for a batch to fill up
    """
    logger.info(f"DB writer started with batch_size={batch_size}, batch_timeout={batch_timeout}s")
    
    # Force reconnect to QuestDB to ensure connection is fresh
    if not qdb_client.connected:
        logger.info("QuestDB not connected, attempting to connect...")
        connection_success = await qdb_client.connect()
        if not connection_success:
            logger.error("Failed to connect to QuestDB. Will retry periodically.")
    else:
        logger.info("QuestDB already connected")
    
    # Ensure metrics file exists and is writeable
    check_metrics_file()
    
    # Initialize metrics with startup values
    update_metrics("full", 0, 0, "STARTUP")
    logger.info("Initialized metrics file")
    
    # Log debug info about the QuestDB connection
    logger.info(f"QuestDB connection: {qdb_client.host}:{qdb_client.port} as {qdb_client.user}")
    
    batches = {
        "full": [],
        "quote": [],
        "ltp": []
    }
    
    # Print debug info about memory structure
    logger.info(f"Initial batches structure: {batches}")
    
    # Stats tracking
    stats = {
        "total_processed": 0,
        "total_batches": 0,
        "errors": 0,
        "last_batch_time": time.time(),
        "events_received": 0,
        "last_log_time": time.time(),
        "consecutive_errors": 0,
        "last_successful_insert": 0
    }
    
    # Additional debugging stats
    debug_stats = {
        "queue_get_count": 0,
        "queue_empty_timeouts": 0,
        "invalid_events": 0,
        "batch_triggers": {
            "size": 0,
            "timeout": 0
        },
        "reconnect_attempts": 0,
        "successful_reconnects": 0
    }
    
    # Connection management
    max_retries = 5
    last_metrics_update = time.time()
    metrics_update_interval = 5  # Update metrics.json every 5 seconds
    
    # Dynamic backoff parameters
    min_wait_time = 0.1  # Start with 100ms wait
    max_wait_time = 30   # Max 30 second wait
    current_wait_time = min_wait_time
    
    # Create a test table marker to ensure tables exist
    tables_created = False
    
    try:
        # Create tables if they don't exist by sending test data
        logger.info("Sending test data to ensure tables exist...")
        test_time = TimestampNanos(int(time.time() * 1_000_000_000))
        
        # Create test data for tables
        test_data_full = {
            "timestamp": int(time.time() * 1_000_000_000),
            "exchange": "TEST",
            "trading_symbol": "TEST",
            "instrument_token": 123456,
            "last_price": 100.0,
            "last_quantity": 1,
            "average_price": 100.0,
            "volume": 100,
            "buy_quantity": 50,
            "sell_quantity": 50,
            "ohlc": {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0}
        }
        
        test_data_quote = {
            "timestamp": int(time.time() * 1_000_000_000),
            "exchange": "TEST",
            "trading_symbol": "TEST",
            "instrument_token": 123456,
            "last_price": 100.0,
            "last_quantity": 1,
            "average_price": 100.0,
            "volume": 100,
            "buy_quantity": 50,
            "sell_quantity": 50,
            "ohlc": {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0}
        }
        
        # Try to insert test data to create tables
        test_batches = {
            "full": [test_data_full],
            "quote": [test_data_quote],
            "ltp": []
        }
        
        if qdb_client.connected:
            logger.info("Inserting test data to create tables...")
            try:
                # Create full table
                success_count = qdb_client.insert_full_ticks_batch(test_batches["full"])
                tables_created = success_count > 0 # type:ignore
                logger.info(f"Test data inserted successfully in full table: {success_count} records")
                
                # Create quote table
                success_count = qdb_client.insert_quote_ticks_batch(test_batches["quote"])
                logger.info(f"Test data inserted successfully in quote table: {success_count} records")
            except Exception as e:
                logger.error(f"Error inserting test data: {e}")
                # We'll continue anyway and try to reconnect if needed
        else:
            logger.warning("QuestDB not connected, cannot insert test data yet")
    except Exception as e:
        logger.error(f"Error setting up tables: {e}")
    
    # Create a reconnection task to periodically check connection
    async def check_connection():
        while True:
            if not qdb_client.connected:
                debug_stats["reconnect_attempts"] += 1
                logger.info(f"Connection check: QuestDB disconnected, attempting to reconnect (attempt {debug_stats['reconnect_attempts']})")
                try:
                    connection_success = await qdb_client.connect()
                    if connection_success:
                        debug_stats["successful_reconnects"] += 1
                        logger.info(f"Successfully reconnected to QuestDB (total successful reconnects: {debug_stats['successful_reconnects']})")
                        
                        # Try to create tables again if needed
                        if not tables_created:
                            try:
                                test_data = [{
                                    "timestamp": int(time.time() * 1_000_000_000),
                                    "exchange": "TEST",
                                    "trading_symbol": "TEST",
                                    "instrument_token": 123456,
                                    "last_price": 100.0,
                                    "last_quantity": 1,
                                    "average_price": 100.0,
                                    "volume": 100,
                                    "buy_quantity": 50,
                                    "sell_quantity": 50,
                                    "ohlc": {"open": 99.0, "high": 101.0, "low": 98.0, "close": 100.0}
                                }]
                                qdb_client.insert_full_ticks_batch(test_data)
                                tables_created = True
                                logger.info("Tables created after reconnection")
                            except Exception as e:
                                logger.error(f"Error creating tables after reconnection: {e}")
                    else:
                        logger.warning("Failed to reconnect to QuestDB")
                except Exception as e:
                    logger.error(f"Error during reconnection attempt: {e}")
            else:
                logger.debug("Connection check: QuestDB connected")
            
            # Check every 30 seconds
            await asyncio.sleep(30)
    
    # Start the connection checker in the background
    connection_check_task = asyncio.create_task(check_connection())
    
    while True:
        try:
            current_batch_size = sum(len(batch) for batch in batches.values())
            queue_size = queue.qsize() if hasattr(queue, 'qsize') else 0
            
            # Log statistics periodically (but not too often)
            current_time = time.time()
            if current_time - stats["last_log_time"] > 30:  # Log every 30 seconds
                logger.info(f"DB writer stats: processed={stats['total_processed']}, "
                           f"received={stats['events_received']}, "
                           f"batches={stats['total_batches']}, errors={stats['errors']}, "
                           f"queue_size={queue_size}, connected={qdb_client.connected}")
                           
                # Log debug stats
                logger.info(f"Debug stats: queue_gets={debug_stats['queue_get_count']}, "
                           f"timeouts={debug_stats['queue_empty_timeouts']}, "
                           f"invalid_events={debug_stats['invalid_events']}, "
                           f"batch_triggers_size={debug_stats['batch_triggers']['size']}, "
                           f"batch_triggers_timeout={debug_stats['batch_triggers']['timeout']}")
                stats["last_log_time"] = current_time
                
                # Report time since last successful insert
                time_since_insert = current_time - stats["last_successful_insert"]
                if stats["last_successful_insert"] > 0:
                    if time_since_insert > 300:  # 5 minutes
                        logger.warning(f"No successful inserts in {time_since_insert:.1f} seconds")
                    else:
                        logger.info(f"Last successful insert: {time_since_insert:.1f} seconds ago")
            
            # Adjust batch timeout based on queue size
            effective_timeout = min(batch_timeout, 0.1) if queue_size > batch_size * 2 else batch_timeout
            
            # Wait for new data with timeout
            try:
                logger.debug(f"Waiting for data (timeout={effective_timeout}s, current_batch={current_batch_size}/{batch_size})")
                event = await asyncio.wait_for(queue.get(), timeout=effective_timeout)
                debug_stats["queue_get_count"] += 1
                
                stats["events_received"] += 1
                stats["consecutive_errors"] = 0  # Reset error counter on successful queue get
                
                # Reset backoff time after successful operations
                current_wait_time = min_wait_time
                
                if isinstance(event, dict) and "stream_type" in event and "data" in event:
                    stream_type = event["stream_type"]
                    data = event["data"]
                    
                    # Log first event details for debugging
                    if stats["events_received"] <= 5:
                        logger.info(f"Received event {stats['events_received']}: stream_type={stream_type}, data={str(data)[:200]}...")
                    
                    if stream_type in batches:
                        batches[stream_type].append(data)
                        logger.debug(f"Added {stream_type} event to batch, now {len(batches[stream_type])}/{batch_size}")
                    else:
                        logger.warning(f"Unknown stream_type: {stream_type}")
                        debug_stats["invalid_events"] += 1
                else:
                    logger.warning(f"Ignoring invalid event: {event}")
                    debug_stats["invalid_events"] += 1
                
                # Mark task as done
                queue.task_done()
                
                # Try to drain queue up to batch size (but don't get too greedy)
                max_drain = min(batch_size * 2, 100)  # Drain at most 100 items at once
                drain_count = 0
                
                while (sum(len(batch) for batch in batches.values()) < batch_size 
                       and drain_count < max_drain 
                       and not queue.empty()):
                    try:
                        event = queue.get_nowait()
                        debug_stats["queue_get_count"] += 1
                        stats["events_received"] += 1
                        drain_count += 1
                        
                        if isinstance(event, dict) and "stream_type" in event and "data" in event:
                            stream_type = event["stream_type"]
                            data = event["data"]
                            if stream_type in batches:
                                batches[stream_type].append(data)
                                logger.debug(f"Added {stream_type} event to batch from queue drain")
                            else:
                                logger.warning(f"Unknown stream_type from queue drain: {stream_type}")
                                debug_stats["invalid_events"] += 1
                        else:
                            logger.warning(f"Invalid event from queue drain: {event}")
                            debug_stats["invalid_events"] += 1
                        queue.task_done()
                    except asyncio.QueueEmpty:
                        break
                
                if drain_count > 0:
                    logger.debug(f"Drained {drain_count} events from queue")
                    
            except asyncio.TimeoutError:
                logger.debug("Batch timeout reached, processing current batch")
                debug_stats["queue_empty_timeouts"] += 1
            
            # Force update metrics periodically
            if time.time() - last_metrics_update > metrics_update_interval:
                queue_size = queue.qsize() if hasattr(queue, 'qsize') else 0
                update_metrics("full", stats['total_processed'], queue_size, "PERIODIC")
                last_metrics_update = time.time()
                logger.debug("Updated metrics (periodic)")
            
            # Process batches if we have data or reached timeout
            # Check either if we have a full batch or if some time has passed since last batch
            batch_total = sum(len(batch) for batch in batches.values())
            time_since_last_batch = time.time() - stats["last_batch_time"]
            
            batch_trigger_reason = None
            if batch_total >= batch_size:
                batch_trigger_reason = "size"
                debug_stats["batch_triggers"]["size"] += 1
            elif batch_total > 0 and time_since_last_batch >= batch_timeout:
                batch_trigger_reason = "timeout"
                debug_stats["batch_triggers"]["timeout"] += 1
                
            if batch_trigger_reason:
                logger.info(f"Processing batches (trigger: {batch_trigger_reason}): full={len(batches['full'])}, quote={len(batches['quote'])}, ltp={len(batches['ltp'])}")
                stats["last_batch_time"] = time.time()
                
                # Process each stream type
                for stream_type, batch in batches.items():
                    if batch:
                        if len(batch) > 0:
                            logger.info(f"Sample batch data: {str(batch[0])[:200]}...")

                        try:
                            # Check connection and reconnect if needed
                            if not qdb_client.connected:
                                logger.warning(f"QuestDB connection lost, attempting to reconnect...")
                                retry_count = 0
                                while retry_count < max_retries and not qdb_client.connected:
                                    try:
                                        await qdb_client.connect()
                                        logger.info("Successfully reconnected to QuestDB")
                                    except Exception as e:
                                        retry_count += 1
                                        wait_time = min(max_wait_time, 2 ** retry_count)
                                        logger.error(f"QuestDB reconnection failed (attempt {retry_count}/{max_retries}): {e}")
                                        if retry_count < max_retries:
                                            logger.info(f"Retrying connection in {wait_time} seconds...")
                                            await asyncio.sleep(wait_time)
                                        else:
                                            logger.warning("Max retries reached. Will try again during next batch.")
                                            break
                            
                            if qdb_client.connected:
                                batch_start_time = time.time()
                                
                                success_count = 0
                                try:
                                    # Insert the data into appropriate table based on stream_type
                                    if stream_type == "full":
                                        if len(batch) > 0:
                                            logger.info(f"Inserting batch of {len(batch)} full tick events into QuestDB")
                                            # Log sample tick for debugging
                                            logger.info(f"Sample tick data: {json.dumps(batch[0])[:200]}...")
                                        success_count = qdb_client.insert_full_ticks_batch(batch)
                                        logger.info(f"Inserted {success_count} full ticks")
                                    elif stream_type == "quote":
                                        if len(batch) > 0:
                                            logger.info(f"Inserting batch of {len(batch)} quote tick events into QuestDB")
                                            # Log sample tick for debugging
                                            logger.info(f"Sample tick data: {json.dumps(batch[0])[:200]}...")
                                        success_count = qdb_client.insert_quote_ticks_batch(batch)
                                        logger.info(f"Inserted {success_count} quote ticks")
                                    elif stream_type == "ltp":
                                        if len(batch) > 0:
                                            logger.info(f"Inserting batch of {len(batch)} ltp tick events into QuestDB")
                                            # Log sample tick for debugging
                                            logger.info(f"Sample tick data: {json.dumps(batch[0])[:200]}...")
                                        success_count = qdb_client.insert_ltp_ticks_batch(batch)
                                        logger.info(f"Inserted {success_count} ltp ticks")
                                    
                                    if success_count and success_count > 0:
                                        stats["total_processed"] += success_count if success_count is not None else 0
                                        stats["total_batches"] += 1
                                        stats["last_successful_insert"] = time.time()
                                        
                                        # Update metrics
                                        update_metrics(stream_type, stats["total_processed"], queue_size, batch[0].get("trading_symbol"))
                                        
                                        logger.info(f"Batch processed in {time.time() - batch_start_time:.2f}s: {success_count}/{len(batch)} {stream_type} events")
                                    else:
                                        logger.warning(f"No {stream_type} events were successfully inserted")
                                        
                                except Exception as e:
                                    stats["errors"] += 1
                                    stats["consecutive_errors"] += 1
                                    logger.error(f"Error inserting {stream_type} batch: {e}")
                                    
                                    # Reset connection on severe errors
                                    if stats["consecutive_errors"] > 5:
                                        logger.warning("Too many consecutive errors, resetting connection...")
                                        qdb_client.connected = False
                            else:
                                logger.warning(f"Skipping {stream_type} batch, QuestDB not connected")
                        except Exception as e:
                            stats["errors"] += 1
                            logger.error(f"Error processing {stream_type} batch: {e}")
                            
                        # Clear batch after processing (whether successful or not)
                        batches[stream_type] = []
        except Exception as e:
            logger.error(f"Unexpected error in db_writer: {e}")
            try:
                # Add a small delay to avoid tight looping in case of persistent errors
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                logger.info("DB writer was cancelled during error recovery sleep")
                break
            
    # Clean up the connection check task when the db_writer task ends
    if connection_check_task and not connection_check_task.done():
        connection_check_task.cancel()
        try:
            await connection_check_task
        except asyncio.CancelledError:
            pass
    
    logger.info("DB writer task ended")

async def print_full_tick(event):
    dt = datetime.fromtimestamp(event["timestamp"] / 1_000_000_000)
    print(f"FULL TICK: {event['trading_symbol']} ({event['exchange']}), Last Price: {event.get('last_price', 0.0)}, Volume: {event.get('volume', 0)} @ {dt}")

async def print_quote_tick(event):
    dt = datetime.fromtimestamp(event["timestamp"] / 1_000_000_000)
    print(f"QUOTE TICK: {event['trading_symbol']} ({event['exchange']}), Last Price: {event.get('last_price', 0.0)}, Open: {event.get('ohlc', {}).get('open', 0.0)} @ {dt}")

async def print_ltp_tick(event):
    dt = datetime.fromtimestamp(event["timestamp"] / 1_000_000_000)
    print(f"LTP TICK: {event['trading_symbol']} ({event['exchange']}), Last Price: {event.get('last_price', 0.0)} @ {dt}")