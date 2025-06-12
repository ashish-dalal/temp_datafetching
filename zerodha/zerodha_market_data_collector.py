"""
Enhanced Market Data Collector for Zerodha Indian Market
Collects data from multiple WebSocket streams in parallel
"""




import asyncio
import json
import logging
import time
import random
from typing import Dict, List, Optional
from kiteconnect import KiteConnect
from questdb.ingress import Sender, TimestampNanos
import os
from dotenv import load_dotenv
import aiohttp
from pathlib import Path
from zerodha_connector import ZerodhaConnector
from zerodha_streams import STREAM_TYPES, TABLE_SCHEMAS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_CONFIGS = {
    'full': {'size': 1000, 'timeout': 0.5},
    'quote': {'size': 2000, 'timeout': 0.2},
    'ltp': {'size': 5000, 'timeout': 0.1}
}

class ConnectionMetrics:
    def __init__(self):
        self.connection_attempts = 0
        self.successful_connections = 0
        self.connection_errors = 0
        self.last_connection_time = None
        self.last_message_times: Dict[str, float] = {}
        self.message_counts: Dict[str, int] = {}
        self.error_counts: Dict[str, int] = {}
        self.processing_times: Dict[str, List[float]] = {}

    def record_connection_attempt(self):
        self.connection_attempts += 1

    def record_connection_success(self):
        self.successful_connections += 1
        self.last_connection_time = time.time()

    def record_connection_error(self, error: str):
        self.connection_errors += 1
        self.error_counts[error] = self.error_counts.get(error, 0) + 1

    def record_message(self, stream_type: str, processing_time: float):
        current_time = time.time()
        self.last_message_times[stream_type] = current_time
        self.message_counts[stream_type] = self.message_counts.get(stream_type, 0) + 1
        if stream_type not in self.processing_times:
            self.processing_times[stream_type] = []
        times = self.processing_times[stream_type]
        times.append(processing_time)
        if len(times) > 1000:
            times.pop(0)

    def get_stream_health(self, stream_type: str) -> dict:
        current_time = time.time()
        last_message_time = self.last_message_times.get(stream_type)
        if not last_message_time:
            return {
                'status': 'unknown',
                'message_count': 0,
                'avg_processing_time': 0,
                'last_message_age': None
            }
        processing_times = self.processing_times.get(stream_type, [])
        avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
        return {
            'status': 'healthy' if current_time - last_message_time < 60 else 'stale',
            'message_count': self.message_counts.get(stream_type, 0),
            'avg_processing_time': avg_processing_time,
            'last_message_age': current_time - last_message_time
        }

    def get_connection_health(self) -> dict:
        return {
            'total_attempts': self.connection_attempts,
            'successful_connections': self.successful_connections,
            'connection_errors': self.connection_errors,
            'error_distribution': self.error_counts,
            'uptime': time.time() - self.last_connection_time if self.last_connection_time else 0
        }

class MarketDataCollector:
    def __init__(self, api_key: str, access_token: str, instrument_tokens: Optional[List[int]] = None, stream_types: Optional[List[str]] = None):
        load_dotenv()
        self.api_key = api_key
        self.access_token = access_token
        self.instrument_tokens = instrument_tokens or []
        self.stream_types = stream_types or ['full']
        self.running = False
        self.kite = KiteConnect(api_key=api_key, access_token=access_token)
        self.host = os.getenv('QUESTDB_HOST', 'qdb3.satyvm.com')
        self.port = int(os.getenv('QUESTDB_PORT', '443'))
        self.username = os.getenv('QUESTDB_USER', '2Cents')
        self.password = os.getenv('QUESTDB_PASSWORD', '2Cents$1012cc')
        self.qdb_conf = (
            f"tcp::addr={self.host}:{self.port};"
            f"username={self.username};"
            f"password={self.password};"
        )
        self.questdb_url = f"https://{self.host}:9000/exec"
        self.queues: Dict[str, asyncio.Queue] = {
            stream_type: asyncio.Queue(maxsize=BATCH_CONFIGS[stream_type]['size'] * 2)
            for stream_type in self.stream_types
        }
        self.metrics = ConnectionMetrics()
        self.last_health_check = time.time()

    async def _fetch_all_instruments(self) -> List[int]:
        try:
            instruments = self.kite.instruments()
            tokens = [
                inst['instrument_token']
                for inst in instruments
                if inst['exchange'] in ['NSE', 'BSE'] and inst['segment'] in ['NSE', 'NFO-FUT']
            ]
            logger.info(f"Fetched {len(tokens)} instruments (NSE/BSE spot and futures)")
            instruments_file = Path(__file__).parent / 'instruments.json'
            with open(instruments_file, 'w') as f:
                json.dump({
                    'timestamp': time.time(),
                    'count': len(tokens),
                    'instruments': tokens
                }, f, indent=2)
            logger.info(f"Saved instruments list to {instruments_file}")
            return tokens
        except Exception as e:
            logger.error(f"Error fetching instruments: {e}")
            raise

    def _build_token_batches(self, batch_size: int = 1000) -> List[List[int]]:
        max_tokens_per_conn = 3000  # Zerodha's limit
        batch_size = min(batch_size, max_tokens_per_conn)
        batches = [self.instrument_tokens[i:i + batch_size] for i in range(0, len(self.instrument_tokens), batch_size)]
        logger.info(f"Created {len(batches)} WebSocket token batches")
        return batches

    async def _process_queue(self, stream_type: str):
        config = BATCH_CONFIGS[stream_type]
        batch_size = config['size']
        batch_timeout = config['timeout']
        table_name = STREAM_TYPES[stream_type]['table']
        batch = []
        last_flush_time = time.time()
        while self.running:
            try:
                current_queue_size = self.queues[stream_type].qsize()
                if current_queue_size > batch_size * 1.5:
                    current_batch_size = min(batch_size * 2, current_queue_size)
                    current_timeout = batch_timeout * 0.5
                else:
                    current_batch_size = batch_size
                    current_timeout = batch_timeout
                try:
                    event = await asyncio.wait_for(self.queues[stream_type].get(), timeout=current_timeout)
                    batch.append(event)
                    while len(batch) < current_batch_size:
                        try:
                            event = self.queues[stream_type].get_nowait()
                            batch.append(event)
                        except asyncio.QueueEmpty:
                            break
                except asyncio.TimeoutError:
                    if not batch:
                        continue
                current_time = time.time()
                should_flush = (
                    len(batch) >= current_batch_size or
                    current_time - last_flush_time >= batch_timeout * 2
                )
                if should_flush and batch:
                    start_time = time.time()
                    with Sender.from_conf(self.qdb_conf) as sender:
                        for event in batch:
                            ts = TimestampNanos(event.get("timestamp", int(time.time() * 1_000_000_000)))
                            if stream_type == 'full':
                                sender.row(
                                    table_name,
                                    symbols={
                                        "exchange": event["exchange"],
                                        "trading_symbol": event["trading_symbol"]
                                    },
                                    columns={
                                        "instrument_token": int(event["instrument_token"]),
                                        "last_price": float(event.get("last_price", 0.0)),
                                        "last_quantity": int(event.get("last_quantity", 0)),
                                        "average_price": float(event.get("average_price", 0.0)),
                                        "volume": int(event.get("volume", 0)),
                                        "buy_quantity": int(event.get("buy_quantity", 0)),
                                        "sell_quantity": int(event.get("sell_quantity", 0)),
                                        "open": float(event.get("ohlc", {}).get("open", 0.0)),
                                        "high": float(event.get("ohlc", {}).get("high", 0.0)),
                                        "low": float(event.get("ohlc", {}).get("low", 0.0)),
                                        "close": float(event.get("ohlc", {}).get("close", 0.0)),
                                        "last_trade_time": int(event.get("last_trade_time", 0)),
                                        "oi": int(event.get("oi", 0)),
                                        "oi_day_high": int(event.get("oi_day_high", 0)),
                                        "oi_day_low": int(event.get("oi_day_low", 0)),
                                        "depth_bids": event.get("depth_bids", "[]"),
                                        "depth_asks": event.get("depth_asks", "[]")
                                    },
                                    at=ts
                                )
                            elif stream_type == 'quote':
                                sender.row(
                                    table_name,
                                    symbols={
                                        "exchange": event["exchange"],
                                        "trading_symbol": event["trading_symbol"]
                                    },
                                    columns={
                                        "instrument_token": int(event["instrument_token"]),
                                        "last_price": float(event.get("last_price", 0.0)),
                                        "last_quantity": int(event.get("last_quantity", 0)),
                                        "average_price": float(event.get("average_price", 0.0)),
                                        "volume": int(event.get("volume", 0)),
                                        "buy_quantity": int(event.get("buy_quantity", 0)),
                                        "sell_quantity": int(event.get("sell_quantity", 0)),
                                        "open": float(event.get("ohlc", {}).get("open", 0.0)),
                                        "high": float(event.get("ohlc", {}).get("high", 0.0)),
                                        "low": float(event.get("ohlc", {}).get("low", 0.0)),
                                        "close": float(event.get("ohlc", {}).get("close", 0.0)),
                                        "last_trade_time": int(event.get("last_trade_time", 0))
                                    },
                                    at=ts
                                )
                            elif stream_type == 'ltp':
                                sender.row(
                                    table_name,
                                    symbols={
                                        "exchange": event["exchange"],
                                        "trading_symbol": event["trading_symbol"]
                                    },
                                    columns={
                                        "instrument_token": int(event["instrument_token"]),
                                        "last_price": float(event.get("last_price", 0.0))
                                    },
                                    at=ts
                                )
                        sender.flush()
                    processing_time = time.time() - start_time
                    logger.info(f"Wrote {len(batch)} {stream_type} events to QuestDB in {processing_time:.3f}s")
                    if processing_time > batch_timeout:
                        logger.warning(f"Slow batch processing for {stream_type}: {processing_time:.3f}s")
                    batch.clear()
                    last_flush_time = current_time
            except Exception as e:
                logger.error(f"Error processing {stream_type} queue: {e}")
                await asyncio.sleep(1)

    async def _create_tables(self):
        try:
            async with aiohttp.ClientSession() as session:
                for table_name, schema in TABLE_SCHEMAS.items():
                    async with session.post(
                        self.questdb_url,
                        json={"query": schema},
                        auth=aiohttp.BasicAuth(self.username, self.password)
                    ) as resp:
                        if resp.status == 200:
                            logger.info(f"Created/verified table: {table_name}")
                        else:
                            logger.error(f"Table creation error: {await resp.text()}")
        except Exception as e:
            logger.error(f"Error setting up QuestDB tables: {e}")
            raise

    async def run(self):
        try:
            if not self.instrument_tokens:
                self.instrument_tokens = await self._fetch_all_instruments()
            logger.info(f"Starting data collection for {len(self.instrument_tokens)} instruments")
            await self._create_tables()
            self.running = True
            health_monitor = asyncio.create_task(self._check_health())
            token_batches = self._build_token_batches()
            processors = [
                asyncio.create_task(self._process_queue(stream_type))
                for stream_type in self.stream_types
            ]
            websocket_tasks = [
                asyncio.create_task(ZerodhaConnector(
                    self.queues[stream_type],
                    tokens,
                    self.api_key,
                    self.access_token,
                    mode=stream_type
                ).connect())
                for stream_type in self.stream_types
                for tokens in token_batches
            ]
            await asyncio.gather(health_monitor, *processors, *websocket_tasks)
        except Exception as e:
            logger.error(f"Error in market data collector: {e}")
            raise
        finally:
            self.running = False

    def stop(self):
        self.running = False

    async def _check_health(self):
        while self.running:
            try:
                current_time = time.time()
                if current_time - self.last_health_check >= 300:
                    self.last_health_check = current_time
                    conn_health = self.metrics.get_connection_health()
                    logger.info(f"Connection Health: {json.dumps(conn_health, indent=2)}")
                    for stream_type in self.stream_types:
                        stream_health = self.metrics.get_stream_health(stream_type)
                        logger.info(f"{stream_type} Health: {json.dumps(stream_health, indent=2)}")
                        if stream_health['status'] == 'stale':
                            logger.warning(f"Stale data for {stream_type}: {stream_health['last_message_age']:.1f}s")
                    for stream_type, queue in self.queues.items():
                        queue_size = queue.qsize()
                        if queue_size > BATCH_CONFIGS[stream_type]['size']:
                            logger.warning(f"Large queue size for {stream_type}: {queue_size} items")
                await asyncio.sleep(60)
            except Exception as e:
                logger.error(f"Error in health check: {e}")
                await asyncio.sleep(60)