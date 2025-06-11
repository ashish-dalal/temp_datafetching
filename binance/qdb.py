import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, Callable, List
import websockets
from questdb.ingress import Sender, TimestampNanos
import os
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'

###############################################################################
# QuestDB Client with Persistent Connection and Batch Write Capability
###############################################################################
class QuestDBClient:
    def __init__(self):
        load_dotenv(ENV_PATH)
        host = os.getenv('QUESTDB_HOST', 'qdb3.satyvm.com')
        port = os.getenv('QUESTDB_PORT', '443')
        self.conf = (
            f"https::addr={host}:{port};"
            f"username={os.getenv('QUESTDB_USER', '2Cents')};"
            f"password={os.getenv('QUESTDB_PASSWORD', '2Cents$1012cc')};"
        )
        self.sender = None
        self.connected = False
        
    async def connect(self):
        """Establish a persistent connection to QuestDB"""
        try:
            self.sender = Sender.from_conf(self.conf)
            self.connected = True
            logger.info("Connected to QuestDB")
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            self.connected = False
            raise

    def insert_mark_prices_batch(self, events: List[dict]):
        """Insert a batch of mark price events into QuestDB using the persistent sender"""
        if not self.connected or self.sender is None:
            raise Exception("Not connected to QuestDB")
        try:
            with Sender.from_conf(self.conf) as sender:
                for data in events:
                    try:
                        # Use event time (E) as the row timestamp (ms -> ns)
                        ts = TimestampNanos(int(data["E"]) * 1_000_000)
                        sender.row(
                            'mark_prices',
                            symbols={'symbol': data['s'].lower()},
                            columns={
                                'mark_price': float(data['p']),
                                'settle_price': float(data['P']),
                                'index_price': float(data['i']),
                                'funding_rate': float(data['r']),
                                'next_funding_time': int(data['T'])  # stored in ms
                            },
                            at=ts
                        )
                    except Exception as e:
                        logger.error(f"Error inserting mark price event: {e}, data: {data}")
                sender.flush()
            logger.debug(f"Inserted batch of {len(events)} mark price events into QuestDB")
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            self.connected = False
            raise

    def insert_ticker_batch(self, events: List[dict]):
        """Insert a batch of ticker events into QuestDB"""
        if not self.connected or self.sender is None:
            raise Exception("Not connected to QuestDB")
        try:
            with Sender.from_conf(self.conf) as sender:
                for data in events:
                    try:
                        ts = TimestampNanos(int(data["E"]) * 1_000_000)
                        sender.row(
                            'tickers',
                            symbols={'symbol': data['s'].lower()},
                            columns={
                                'price': float(data['c']),
                                'price_change': float(data['p']), 
                                'price_change_percent': float(data['P']),
                                'weighted_avg_price': float(data['w']),
                                'open_price': float(data['o']),
                                'high_price': float(data['h']),
                                'low_price': float(data['l']),
                                'volume': float(data['v']),
                                'quote_volume': float(data['q']),
                                'num_trades': int(data['n']),
                            },
                            at=ts
                        )
                    except Exception as e:
                        logger.error(f"Error inserting ticker event: {e}, data: {data}")
                sender.flush()
            logger.debug(f"Inserted batch of {len(events)} ticker events into QuestDB")
        except Exception as e:
            logger.error(f"Error inserting ticker batch: {e}")
            self.connected = False
            raise

    def insert_agg_trades_batch(self, events: List[dict]):
        """Insert a batch of aggregated trade events into QuestDB"""
        if not self.connected or self.sender is None:
            raise Exception("Not connected to QuestDB")
        try:
            with Sender.from_conf(self.conf) as sender:
                for data in events:
                    try:
                        ts = TimestampNanos(int(data["E"]) * 1_000_000)
                        # Convert string 'true'/'false' or boolean True/False to actual boolean for is_buyer_maker
                        is_buyer_maker = False
                        if isinstance(data['m'], bool):
                            is_buyer_maker = data['m']
                        elif isinstance(data['m'], str):
                            is_buyer_maker = data['m'].lower() == 'true'
                        else:
                            is_buyer_maker = bool(data['m'])
                            
                        sender.row(
                            'agg_trades',
                            symbols={'symbol': data['s'].lower()},
                            columns={
                                'agg_id': int(data['a']),
                                'price': float(data['p']),
                                'quantity': float(data['q']),
                                'first_trade_id': int(data['f']),
                                'last_trade_id': int(data['l']),
                                'trade_time': int(data['T']),
                                'is_buyer_maker': is_buyer_maker,  # Use boolean directly in columns
                            },
                            at=ts
                        )
                    except Exception as e:
                        logger.error(f"Error inserting agg trade event: {e}, data: {data}")
                sender.flush()
            logger.debug(f"Inserted batch of {len(events)} agg trade events into QuestDB")
        except Exception as e:
            logger.error(f"Error inserting agg trades batch: {e}")
            self.connected = False
            raise

    def insert_book_ticker_batch(self, events: List[dict]):
        """Insert a batch of book ticker events into QuestDB"""
        if not self.connected or self.sender is None:
            raise Exception("Not connected to QuestDB")
        try:
            with Sender.from_conf(self.conf) as sender:
                for data in events:
                    try:
                        ts = TimestampNanos(int(data["E"]) * 1_000_000)
                        sender.row(
                            'book_tickers',
                            symbols={'symbol': data['s'].lower()},
                            columns={
                                'bid_price': float(data['b']),
                                'bid_qty': float(data['B']),
                                'ask_price': float(data['a']),
                                'ask_qty': float(data['A']),
                            },
                            at=ts
                        )
                    except Exception as e:
                        logger.error(f"Error inserting book ticker event: {e}, data: {data}")
                sender.flush()
            logger.debug(f"Inserted batch of {len(events)} book ticker events into QuestDB")
        except Exception as e:
            logger.error(f"Error inserting book ticker batch: {e}")
            self.connected = False
            raise

    def insert_klines_batch(self, events: List[dict]):
        """Insert a batch of kline events into QuestDB"""
        if not self.connected or self.sender is None:
            raise Exception("Not connected to QuestDB")
        try:
            with Sender.from_conf(self.conf) as sender:
                for data in events:
                    try:
                        if "k" not in data:
                            continue
                        
                        k = data["k"]
                        # Convert string 'true'/'false' or boolean True/False to actual boolean for is_closed
                        is_closed = False
                        if isinstance(k['x'], bool):
                            is_closed = k['x']
                        elif isinstance(k['x'], str):
                            is_closed = k['x'].lower() == 'true'
                        else:
                            is_closed = bool(k['x'])
                            
                        # Use kline start time as timestamp
                        ts = TimestampNanos(int(k["t"]) * 1_000_000)
                        sender.row(
                            'klines',
                            symbols={
                                'symbol': k['s'].lower(),
                                'interval': k['i'],
                            },
                            columns={
                                'open_time': int(k['t']),
                                'close_time': int(k['T']),
                                'open': float(k['o']),
                                'high': float(k['h']),
                                'low': float(k['l']),
                                'close': float(k['c']),
                                'volume': float(k['v']),
                                'quote_volume': float(k['q']),
                                'num_trades': int(k['n']),
                                'taker_buy_vol': float(k['V']),
                                'taker_buy_quote_vol': float(k['Q']),
                                'is_closed': is_closed,  # Use boolean directly in columns
                            },
                            at=ts
                        )
                    except Exception as e:
                        logger.error(f"Error inserting kline event: {e}, data: {data}")
                sender.flush()
            logger.debug(f"Inserted batch of {len(events)} kline events into QuestDB")
        except Exception as e:
            logger.error(f"Error inserting klines batch: {e}")
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

###############################################################################
# Background DB Writer Task: Consumes from Queue and Uploads to QuestDB
###############################################################################
async def db_writer(queue: asyncio.Queue, qdb_client: QuestDBClient, batch_size: int = 100, batch_timeout: float = 0.5):
    logger.info("DB writer started")
    
    # Initialize batches for each type of event
    batches = {
        "markPrice": [],
        "ticker": [],
        "aggTrade": [],
        "bookTicker": [],
        "kline": []
    }
    
    # Maximum number of reconnection attempts
    max_retries = 5
    
    while True:
        try:
            try:
                # Wait for at least one event, with timeout
                event = await asyncio.wait_for(queue.get(), timeout=batch_timeout)
                if "stream_type" in event and "data" in event:
                    stream_type = event["stream_type"]
                    data = event["data"]
                    
                    # Add to appropriate batch based on stream_type
                    if stream_type in batches:
                        batches[stream_type].append(data)
                    else:
                        logger.warning(f"Unknown stream_type: {stream_type}")
                
                # Drain queue up to batch_size without waiting
                while sum(len(batch) for batch in batches.values()) < batch_size:
                    try:
                        event = queue.get_nowait()
                        if "stream_type" in event and "data" in event:
                            stream_type = event["stream_type"]
                            data = event["data"]
                            
                            if stream_type in batches:
                                batches[stream_type].append(data)
                            else:
                                logger.warning(f"Unknown stream_type: {stream_type}")
                    except asyncio.QueueEmpty:
                        break
            except asyncio.TimeoutError:
                # Timeout reached, process current batches if not empty
                pass
            
            # Process each batch if it has items
            for stream_type, batch in batches.items():
                if batch:
                    try:
                        # Check if we're still connected before inserting
                        if not qdb_client.connected:
                            logger.warning("QuestDB connection lost, attempting to reconnect...")
                            retry_count = 0
                            while retry_count < max_retries and not qdb_client.connected:
                                try:
                                    await qdb_client.connect()
                                    logger.info("Successfully reconnected to QuestDB")
                                except Exception as e:
                                    retry_count += 1
                                    wait_time = 2 ** retry_count  # Exponential backoff
                                    logger.error(f"QuestDB reconnection failed (attempt {retry_count}/{max_retries}): {e}")
                                    if retry_count < max_retries:
                                        logger.info(f"Retrying connection in {wait_time} seconds...")
                                        await asyncio.sleep(wait_time)
                                    else:
                                        logger.warning("Max retries reached. Will try again later.")
                                        break
                        
                        # Only insert if we have a connection
                        if qdb_client.connected:
                            if stream_type == "markPrice":
                                qdb_client.insert_mark_prices_batch(batch)
                            elif stream_type == "ticker":
                                qdb_client.insert_ticker_batch(batch)
                            elif stream_type == "aggTrade":
                                qdb_client.insert_agg_trades_batch(batch)
                            elif stream_type == "bookTicker":
                                qdb_client.insert_book_ticker_batch(batch)
                            elif stream_type == "kline":
                                qdb_client.insert_klines_batch(batch)
                        else:
                            logger.warning(f"Skipping insertion of {len(batch)} {stream_type} events due to no database connection")
                    except Exception as e:
                        logger.error(f"Error inserting {stream_type} batch: {e}")
                    finally:
                        batch.clear()
        except Exception as e:
            logger.error(f"Error in db_writer: {e}")
            await asyncio.sleep(1)

###############################################################################
# Example Callback: Print Mark Price Event
###############################################################################
async def print_mark_price(event):
    dt = datetime.fromtimestamp(int(event["E"]) / 1000)
    print(f"Symbol: {event['s']}, Mark Price: {event['p']}, Funding Rate: {event['r']} @ {dt}")

###############################################################################
# Example Callback: Print Ticker Event
###############################################################################
async def print_ticker(event):
    dt = datetime.fromtimestamp(int(event["E"]) / 1000)
    print(f"TICKER: {event['s']}, Price: {event['c']}, 24h Change: {event['p']}({event['P']}%), Volume: {event['v']} @ {dt}")

###############################################################################
# Example Callback: Print Aggregated Trades
###############################################################################
async def print_agg_trades(event):
    dt = datetime.fromtimestamp(int(event["E"]) / 1000)
    print(f"AGG TRADE: {event['s']}, Price: {event['p']}, Quantity: {event['q']}, Is Buyer Maker: {event['m']} @ {dt}")

###############################################################################
# Example Callback: Print Book Ticker
###############################################################################
async def print_book_ticker(event):
    dt = datetime.fromtimestamp(int(event["E"]) / 1000)
    print(f"BOOK: {event['s']}, Best Bid: {event['b']}({event['B']}), Best Ask: {event['a']}({event['A']}) @ {dt}")

###############################################################################
# Example Callback: Print Klines (Candlestick)
###############################################################################
async def print_klines(event):
    if "k" in event:
        k = event["k"]
        dt = datetime.fromtimestamp(int(k["t"]) / 1000)
        print(f"KLINE: {k['s']}, Interval: {k['i']}, Open: {k['o']}, High: {k['h']}, Low: {k['l']}, Close: {k['c']}, Volume: {k['v']}, Closed: {k['x']} @ {dt}")