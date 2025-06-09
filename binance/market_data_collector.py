"""
Enhanced Market Data Collector for Binance Futures
Collects data from multiple WebSocket streams in parallel
"""




import asyncio
import json
import logging
import time
import random
from typing import Dict, List, Optional, Set
import websockets
from questdb.ingress import Sender, TimestampNanos
import os
from dotenv import load_dotenv
import aiohttp
from pathlib import Path

from binance_streams import STREAM_TYPES, TABLE_SCHEMAS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BATCH_CONFIGS = {
    'markPrice': {'size': 1000, 'timeout': 0.5},
    'bookTicker': {'size': 2000, 'timeout': 0.2},
    'kline': {'size': 500, 'timeout': 1.0},
    'ticker': {'size': 200, 'timeout': 1.0},
    'aggTrade': {'size': 2000, 'timeout': 0.1}
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
    def __init__(self, symbols: Optional[List[str]] = None, stream_types: Optional[List[str]] = None):
        load_dotenv()
        self.symbols = []
        self.stream_types = stream_types or ['markPrice', 'kline', 'aggTrade', 'bookTicker', 'ticker']
        self.running = False
        self.ws_url = "wss://fstream.binance.com/stream?streams="
        self.rest_api_url = "https://fapi.binance.com"
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
        self.active_subscriptions: Set[str] = set()
        self.websockets: List[websockets.WebSocketClientProtocol] = []
        self._provided_symbols = [s.lower() for s in symbols] if symbols else None
        self.metrics = ConnectionMetrics()
        self.last_health_check = time.time()

    async def _fetch_all_symbols(self) -> List[str]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.rest_api_url}/fapi/v1/exchangeInfo") as response:
                    if response.status == 200:
                        data = await response.json()
                        symbols = [
                            symbol['symbol'].lower()
                            for symbol in data['symbols']
                            if symbol['contractType'] == 'PERPETUAL' and symbol['status'] == 'TRADING'
                        ]
                        logger.info(f"Fetched {len(symbols)} available trading pairs")
                        symbols_file = Path(__file__).parent / 'symbols.json'
                        with open(symbols_file, 'w') as f:
                            json.dump({
                                'timestamp': time.time(),
                                'count': len(symbols),
                                'symbols': symbols
                            }, f, indent=2)
                        logger.info(f"Saved symbols list to {symbols_file}")
                        return symbols
                    else:
                        raise Exception(f"Failed to fetch symbols: {await response.text()}")
        except Exception as e:
            logger.error(f"Error fetching symbols: {e}")
            raise

    def _build_stream_urls(self, batch_size: int = 50) -> List[str]:
        all_streams = []
        current_batch = []
        for symbol in self.symbols:
            symbol_streams = []
            for stream_type in self.stream_types:
                stream_info = STREAM_TYPES[stream_type]
                if stream_type == "kline":
                    for interval in stream_info["intervals"]:
                        stream = f"{symbol}{stream_info['suffix']}{interval}"
                        symbol_streams.append(stream)
                        self.active_subscriptions.add(f"{symbol}@kline_{interval}")
                else:
                    stream = f"{symbol}{stream_info['suffix']}"
                    symbol_streams.append(stream)
                    self.active_subscriptions.add(f"{symbol}@{stream_type}")
            if len(current_batch) + len(symbol_streams) > batch_size:
                if current_batch:
                    all_streams.append(self.ws_url + "/".join(current_batch))
                current_batch = symbol_streams
            else:
                current_batch.extend(symbol_streams)
        if current_batch:
            all_streams.append(self.ws_url + "/".join(current_batch))
        logger.info(f"Created {len(all_streams)} WebSocket stream batches")
        return all_streams

    async def _handle_message(self, message: str):
        try:
            data = json.loads(message)
            if "stream" in data and "data" in data:
                stream = data["stream"]
                event = data["data"]
                stream_type = None
                if "markPrice" in stream:
                    stream_type = 'markPrice'
                    await self.queues['markPrice'].put(event)
                elif "kline" in stream:
                    stream_type = 'kline'
                    await self.queues['kline'].put(event)
                elif "aggTrade" in stream:
                    stream_type = 'aggTrade'
                    await self.queues['aggTrade'].put(event)
                elif "bookTicker" in stream:
                    stream_type = 'bookTicker'
                    await self.queues['bookTicker'].put(event)
                elif "ticker" in stream:
                    stream_type = 'ticker'
                    await self.queues['ticker'].put(event)
                if stream_type:
                    logger.debug(f"Processed {stream_type} event for {event.get('s', 'unknown')}")
                else:
                    logger.warning(f"Unknown stream type: {stream}")
        except json.JSONDecodeError:
            logger.error(f"Failed to parse message: {message}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

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

    async def _maintain_websocket(self, ws_url: str):
        base_delay = 1
        max_delay = 300
        current_delay = base_delay
        while self.running:
            try:
                self.metrics.record_connection_attempt()
                async with websockets.connect(ws_url) as websocket:
                    logger.info(f"Connected to WebSocket stream batch")
                    self.websockets.append(websocket)
                    self.metrics.record_connection_success()
                    current_delay = base_delay
                    while self.running:
                        try:
                            message = await websocket.recv()
                            start_time = time.time()
                            await self._handle_message(message)
                            processing_time = time.time() - start_time
                            if "stream" in message:
                                stream_type = message.split("@")[1].split("_")[0]
                                self.metrics.record_message(stream_type, processing_time)
                        except websockets.ConnectionClosed:
                            logger.warning("WebSocket connection closed")
                            self.metrics.record_connection_error("connection_closed")
                            break
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            self.metrics.record_connection_error(str(e))
                    if websocket in self.websockets:
                        self.websockets.remove(websocket)
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}")
                self.metrics.record_connection_error(str(e))
                delay = min(current_delay * (1 + random.random() * 0.1), max_delay)
                logger.info(f"Waiting {delay:.1f}s before reconnecting")
                await asyncio.sleep(delay)
                current_delay = min(current_delay * 2, max_delay)

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
                            ts = TimestampNanos(int(event.get("E", event.get("T", time.time() * 1000)) * 1_000_000))
                            if stream_type == 'markPrice':
                                sender.row(
                                    table_name,
                                    symbols={"symbol": event["s"].lower()},
                                    columns={
                                        "mark_price": float(event["p"]),
                                        "funding_rate": float(event["r"]),
                                        "next_funding_time": int(event["T"]),
                                        "index_price": float(event.get("i", 0.0))
                                    },
                                    at=ts
                                )
                            elif stream_type == 'kline':
                                k = event["k"]
                                sender.row(
                                    table_name,
                                    symbols={"symbol": event["s"].lower(), "interval": k["i"]},
                                    columns={
                                        "open_time": int(k["t"]),
                                        "close_time": int(k["T"]),
                                        "open": float(k["o"]),
                                        "high": float(k["h"]),
                                        "low": float(k["l"]),
                                        "close": float(k["c"]),
                                        "volume": float(k["v"]),
                                        "quote_volume": float(k["q"]),
                                        "trades": int(k["n"]),
                                        "taker_buy_volume": float(k["V"]),
                                        "taker_buy_quote_volume": float(k["Q"]),
                                        "is_closed": k["x"]
                                    },
                                    at=ts
                                )
                            elif stream_type == 'aggTrade':
                                sender.row(
                                    table_name,
                                    symbols={"symbol": event["s"].lower()},
                                    columns={
                                        "price": float(event["p"]),
                                        "quantity": float(event["q"]),
                                        "first_trade_id": int(event["f"]),
                                        "last_trade_id": int(event["l"]),
                                        "transaction_time": int(event["T"]),
                                        "is_buyer_maker": event["m"]
                                    },
                                    at=ts
                                )
                            elif stream_type == 'bookTicker':
                                sender.row(
                                    table_name,
                                    symbols={"symbol": event["s"].lower()},
                                    columns={
                                        "bid_price": float(event["b"]),
                                        "bid_qty": float(event["B"]),
                                        "ask_price": float(event["a"]),
                                        "ask_qty": float(event["A"])
                                    },
                                    at=ts
                                )
                            elif stream_type == 'ticker':
                                sender.row(
                                    table_name,
                                    symbols={"symbol": event["s"].lower()},
                                    columns={
                                        "price_change": float(event["p"]),
                                        "price_change_percent": float(event["P"]),
                                        "weighted_avg_price": float(event["w"]),
                                        "last_price": float(event["c"]),
                                        "last_quantity": float(event["Q"]),
                                        "open_price": float(event["o"]),
                                        "high_price": float(event["h"]),
                                        "low_price": float(event["l"]),
                                        "base_volume": float(event["v"]),
                                        "quote_volume": float(event["q"]),
                                        "open_time": int(event["O"]),
                                        "close_time": int(event["C"]),
                                        "first_trade_id": int(event["F"]),
                                        "last_trade_id": int(event["L"]),
                                        "trade_count": int(event["n"])
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
                    if table_name in ['mark_prices', 'klines', 'agg_trades', 'book_tickers', 'tickers']:
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
            if not self._provided_symbols:
                self.symbols = await self._fetch_all_symbols()
            else:
                self.symbols = self._provided_symbols
            logger.info(f"Starting data collection for {len(self.symbols)} symbols")
            await self._create_tables()
            self.running = True
            health_monitor = asyncio.create_task(self._check_health())
            ws_urls = self._build_stream_urls()
            processors = [
                asyncio.create_task(self._process_queue(stream_type))
                for stream_type in self.stream_types
            ]
            websocket_tasks = [
                asyncio.create_task(self._maintain_websocket(url))
                for url in ws_urls
            ]
            await asyncio.gather(health_monitor, *processors, *websocket_tasks)
        except Exception as e:
            logger.error(f"Error in market data collector: {e}")
            raise
        finally:
            self.running = False
            for ws in self.websockets:
                if not ws.closed:
                    await ws.close()

    def stop(self):
        self.running = False