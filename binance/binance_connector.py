import asyncio
import json
import logging
from typing import Callable
import websockets
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinanceConnector:
    def __init__(self, queue: asyncio.Queue, symbols: list):
        self.symbols = [symbol.lower() for symbol in symbols]
        self.queue = queue
        self.callbacks = []
        self.running = False
        self.ws_urls = []
        
        # Define streams per symbol
        streams_per_symbol = [
            "{symbol}@markPrice@1s",
            "{symbol}@ticker",
            "{symbol}@aggTrade",
            "{symbol}@bookTicker",
            "{symbol}@kline_1m"
        ]
        
        # Split symbols into smaller chunks to avoid HTTP 414 URI Too Long
        max_streams_per_conn = 250  # ~50 symbols × 5 streams
        symbols_per_conn = max_streams_per_conn // len(streams_per_symbol)  # ~50 symbols
        for i in range(0, len(self.symbols), symbols_per_conn):
            chunk_symbols = self.symbols[i:i + symbols_per_conn]
            streams = []
            for symbol in chunk_symbols:
                for stream_template in streams_per_symbol:
                    streams.append(stream_template.format(symbol=symbol))
            ws_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
            self.ws_urls.append(ws_url)
        logger.info(f"Created {len(self.ws_urls)} WebSocket connections for {len(self.symbols)} symbols")

    def add_callback(self, callback: Callable):
        """Register a callback to process each event"""
        self.callbacks.append(callback)

    async def connect(self):
        """Maintain WebSocket connections with reconnection logic"""
        max_reconnect_delay = 60
        tasks = []

        async def connect_single(ws_url):
            reconnect_delay = 1
            while self.running:
                try:
                    async with websockets.connect(ws_url) as websocket:
                        logger.info(f"Connected to Binance WebSocket: {ws_url[:100]}...")
                        reconnect_delay = 1
                        while self.running:
                            try:
                                message = await websocket.recv()
                                logger.debug(f"Received message: {message}")
                                await self._handle_message(message)
                            except websockets.ConnectionClosed:
                                logger.warning("WebSocket connection closed")
                                break
                except Exception as e:
                    logger.error(f"WebSocket error: {e}")
                    if self.running:
                        logger.info(f"Reconnecting in {reconnect_delay}s")
                        await asyncio.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

        for ws_url in self.ws_urls:
            tasks.append(asyncio.create_task(connect_single(ws_url)))
        await asyncio.gather(*tasks)

    async def _handle_message(self, message: str):
        """Parse and enqueue stream events"""
        try:
            msg = json.loads(message)
            if "stream" in msg and "data" in msg:
                stream = msg["stream"]
                event = msg["data"]
                
                # Determine stream type based on stream name
                if "markPrice" in stream:
                    stream_type = "markPrice"
                elif "ticker" in stream:
                    stream_type = "ticker"
                elif "aggTrade" in stream:
                    stream_type = "aggTrade"
                elif "bookTicker" in stream:
                    stream_type = "bookTicker"
                elif "kline" in stream:
                    stream_type = "kline"
                else:
                    logger.warning(f"Unknown stream: {stream}")
                    return
                
                # Log received message for debugging
                logger.debug(f"Received {stream_type} event: {event}")
                
                # Ensure timestamp fields are consistent
                if "E" not in event:
                    event["E"] = int(time.time() * 1000)
                
                # Add T field for QuestDB timestamp if not present
                event["T"] = int(event.get("T", event.get("E", int(time.time() * 1000)))) * 1_000_000
                
                # Add symbol if it's not in the event (some events don't include it)
                if "s" not in event:
                    # Extract symbol from stream name (e.g., btcusdt@kline_1m -> btcusdt)
                    symbol = stream.split('@')[0].upper()
                    event["s"] = symbol
                
                # Normalize based on stream type to ensure all required fields exist
                if stream_type == "markPrice":
                    if "r" not in event:  # funding rate
                        event["r"] = "0"
                    if "p" not in event:  # mark price
                        event["p"] = event.get("c", "0")
                    if "P" not in event:  # settle price
                        event["P"] = "0"
                    if "i" not in event:  # index price
                        event["i"] = "0"
                
                elif stream_type == "ticker":
                    for field in ["p", "P", "w", "c", "Q", "o", "h", "l", "v", "q"]:
                        if field not in event:
                            event[field] = "0"
                    for field in ["O", "C", "F", "L", "n"]:
                        if field not in event:
                            event[field] = 0
                
                elif stream_type =="aggTrade":
                    for field in ["p", "q"]:
                        if field not in event:
                            event[field] = "0"
                    for field in ["f", "l", "a"]:
                        if field not in event:
                            event[field] = 0
                    if "m" not in event:
                        event["m"] = False
                    elif isinstance(event["m"], str):
                        event["m"] = event["m"].lower() == "true"
                
                elif stream_type == "bookTicker":
                    for field in ["b", "B", "a", "A"]:
                        if field not in event:
                            event[field] = "0"
                
                elif stream_type == "kline":
                    if "k" not in event:
                        event["k"] = {}
                    k = event["k"]
                    for field in ["t", "T", "s", "i", "o", "c", "h", "l", "v", "q", "n", "V", "Q"]:
                        if field not in k:
                            k[field] = "0" if field in ["o", "c", "h", "l", "v", "q", "V", "Q"] else 0
                    if "x" not in k:
                        k["x"] = False
                    elif isinstance(k["x"], str):
                        k["x"] = k["x"].lower() == "true"
                    if "s" not in k:
                        k["s"] = event["s"]
                    if "i" not in k:
                        k["i"] = "1m"
                
                # Enqueue event with stream type
                await self.queue.put({"stream_type": stream_type, "data": event})
                
                # Trigger callbacks
                for callback in self.callbacks:
                    try:
                        callback_name = callback.__name__
                        if callback_name == f"print_{stream_type}" or (
                            stream_type == "markPrice" and callback_name == "print_mark_price"
                        ) or (
                            stream_type == "aggTrade" and callback_name == "print_agg_trades"
                        ) or (
                            stream_type == "bookTicker" and callback_name == "print_book_ticker"
                        ):
                            await callback(event)
                    except Exception as e:
                        logger.error(f"Callback {callback.__name__} failed: {e}")
            else:
                logger.warning(f"Unexpected message format: {msg}")
        except json.JSONDecodeError:
            logger.error(f"Failed to parse message: {message}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")

    def start(self):
        """Start the connector"""
        self.running = True

    def stop(self):
        """Stop the connector"""
        self.running = False