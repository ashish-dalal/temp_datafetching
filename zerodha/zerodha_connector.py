import asyncio
import json
import logging
from typing import Callable, List
from kiteconnect import KiteTicker
import time
from datetime import datetime
import threading
import websockets
import random
import concurrent.futures



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ZerodhaConnector:
    def __init__(self, queue: asyncio.Queue, instrument_tokens: List[int],instrument_mapping: dict, api_key: str, access_token: str, mode: str = "quote"):
        self.instrument_tokens = instrument_tokens
        self.instrument_mapping = instrument_mapping
        self.queue = queue
        self.api_key = api_key
        self.access_token = access_token
        self.mode = mode
        self.running = False
        self.kws = None
        self.callbacks = []
        self._connection_task = None
        self._loop = None
        self.ws = None
        self.ws_url = None
        self.headers = None
        self._last_connection_time = 0
        self._connection_cooldown = 30  # Increased from 5 to 30 seconds between connection attempts
        self._max_instruments_per_connection = 250  # Reduced from 500 to 250
        self._connection_semaphore = asyncio.Semaphore(2)  # Reduced from 5 to 2 max concurrent connections
        # Tracking for 429 errors
        self._rate_limited = False
        self._rate_limit_start = 0
        self._rate_limit_cooldown = 120  # 2 minutes cooldown after a 429 error
        # Store asyncio event loop for correctly handling callbacks from different threads
        self._event_loop = None
        # Stats tracking
        self.stats = {
            "ticks_received": 0,
            "ticks_processed": 0,
            "queue_errors": 0,
            "processing_errors": 0
        }

    def add_callback(self, callback: Callable):
        """Register a callback to process each tick"""
        self.callbacks.append(callback)

    async def connect(self):
        """Maintain WebSocket connection with improved rate limiting"""
        # Store the event loop for later use
        self._event_loop = asyncio.get_running_loop()
        
        max_reconnect_delay = 600  # Increased from 300 to 600 seconds (10 minutes max delay)
        reconnect_delay = 10  # Increased from 1 to 10 seconds base delay
        consecutive_failures = 0

        while self.running:
            try:
                current_time = time.time()
                time_since_last_connection = current_time - self._last_connection_time
                
                # Check if we're in rate limit cooldown
                if self._rate_limited:
                    time_since_rate_limit = current_time - self._rate_limit_start
                    if time_since_rate_limit < self._rate_limit_cooldown:
                        wait_time = self._rate_limit_cooldown - time_since_rate_limit
                        logger.warning(f"Rate limit cooldown: waiting {wait_time:.1f} seconds before reconnecting")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        self._rate_limited = False
                
                # Enforce cooldown period between connection attempts
                if time_since_last_connection < self._connection_cooldown:
                    cooldown_wait = self._connection_cooldown - time_since_last_connection
                    logger.info(f"Connection cooldown: waiting {cooldown_wait:.1f} seconds")
                    await asyncio.sleep(cooldown_wait)
                
                # Use semaphore to limit concurrent connections
                async with self._connection_semaphore:
                    if self.kws:
                        try:
                            self.kws.close()
                        except:
                            pass
                        self.kws = None

                    # Add jitter to prevent thundering herd problem
                    jitter_delay = random.uniform(0.5, 2.0)
                    await asyncio.sleep(jitter_delay)
                    
                    self.kws = KiteTicker(self.api_key, self.access_token)
                    self.kws.on_ticks = self._on_ticks
                    self.kws.on_connect = self._on_connect
                    self.kws.on_error = self._on_error
                    self.kws.on_close = self._on_close
                    self.kws.on_reconnect = self._on_reconnect

                    # Connect and subscribe
                    logger.info(f"Connecting to Zerodha WebSocket...")
                    self.kws.connect(threaded=True)
                    self._last_connection_time = time.time()
                    
                    # Wait a bit to ensure connection is established
                    await asyncio.sleep(5)
                    
                    if self.kws and self.kws.is_connected():
                        logger.info(f"Connected to Zerodha WebSocket for {len(self.instrument_tokens)} instruments")
                        # Reset failure count on successful connection
                        consecutive_failures = 0
                        reconnect_delay = 10  # Reset to base delay
                        
                        # Keep the connection alive
                        while self.running and self.kws and self.kws.is_connected():
                            await asyncio.sleep(5)
                        
                        if self.running:
                            logger.warning("WebSocket connection closed, attempting to reconnect...")
                    else:
                        logger.error("Failed to establish connection, will retry")
                        consecutive_failures += 1
                        
            except Exception as e:
                consecutive_failures += 1
                error_str = str(e)
                logger.error(f"WebSocket error: {error_str}")
                
                # Check for rate limit errors
                if "429" in error_str or "TooManyRequests" in error_str:
                    logger.warning("Rate limit hit (429 Too Many Requests)")
                    self._rate_limited = True
                    self._rate_limit_start = time.time()
                    # Skip the rest of this iteration and go into rate limit cooldown
                    continue
                
                if self.running:
                    # Exponential backoff with jitter
                    jitter = random.uniform(0.8, 1.2)
                    delay = min(reconnect_delay * (1.5 ** consecutive_failures) * jitter, max_reconnect_delay)
                    
                    logger.info(f"Reconnecting in {delay:.1f}s (attempt {consecutive_failures})")
                    await asyncio.sleep(delay)

    def _on_connect(self, ws, response):
        """Called when WebSocket connection is established"""
        try:
            chunk_size = 25
            logger.info(f"Subscribing to {len(self.instrument_tokens)} instruments in '{self.mode}' mode using chunks of {chunk_size}")
            
            for i in range(0, len(self.instrument_tokens), chunk_size):
                chunk = self.instrument_tokens[i:i + chunk_size]
                ws.subscribe(chunk)
                ws.set_mode(self.mode, chunk)
                time.sleep(1.5)
                logger.info(f"Subscribed to chunk {i//chunk_size + 1}/{(len(self.instrument_tokens)+chunk_size-1)//chunk_size} in '{self.mode}' mode")
            
            logger.info(f"Successfully subscribed to all {len(self.instrument_tokens)} instruments in '{self.mode}' mode")
        except Exception as e:
            logger.error(f"Error in _on_connect: {e}")

    def _on_ticks(self, ws, ticks):
        """Handle incoming ticks from Zerodha WebSocket API"""
        try:
            if not ticks:
                logger.warning("Received empty ticks list")
                return

            tick_count = len(ticks)
            self.stats["ticks_received"] += tick_count
            logger.info(f"Received {tick_count} ticks (total: {self.stats['ticks_received']}) in '{self.mode}' mode")

            # Check queue size
            queue_size = self.queue.qsize()
            if queue_size > 5000:
                logger.warning(f"Queue is too full ({queue_size} items). Processing only 1 tick.")
                ticks = ticks[:1]

            # Normalize ticks with the configured mode
            normalized_ticks = []
            for tick in ticks:
                try:
                    normalized_tick = tick.copy()
                    normalized_tick["timestamp"] = int(time.time() * 1_000_000_000)
                    
                    instrument_token = normalized_tick.get("instrument_token")
                    if instrument_token in self.instrument_mapping:
                        normalized_tick["exchange"] = self.instrument_mapping[instrument_token]["exchange"]
                        normalized_tick["trading_symbol"] = self.instrument_mapping[instrument_token]["trading_symbol"]
                    else:
                        normalized_tick["exchange"] = "UNKNOWN"
                        normalized_tick["trading_symbol"] = normalized_tick.get("tradingsymbol", "")
                    
                    if "last_trade_time" in normalized_tick and isinstance(normalized_tick["last_trade_time"], datetime):
                        normalized_tick["last_trade_time"] = int(normalized_tick["last_trade_time"].timestamp() * 1_000_000_000)
                    if "exchange_timestamp" in normalized_tick and isinstance(normalized_tick["exchange_timestamp"], datetime):
                        normalized_tick["exchange_timestamp"] = int(normalized_tick["exchange_timestamp"].timestamp() * 1_000_000_000)

                    normalized_ticks.append(normalized_tick)
                except Exception as e:
                    logger.error(f"Error normalizing tick: {e}")

            # Send all ticks to the event loop with the configured mode
            if normalized_ticks:
                batch_event = {
                    "stream_type": self.mode,  # Always use the configured mode
                    "batch": normalized_ticks,
                    "timestamp": int(time.time() * 1_000_000_000)
                }
                if self._event_loop:
                    asyncio.run_coroutine_threadsafe(self._process_tick_batch(batch_event), self._event_loop)
                else:
                    logger.error("No event loop available for processing ticks")

        except Exception as e:
            logger.error(f"Error in _on_ticks: {e}")
            self.stats["processing_errors"] += 1
            
    async def _process_tick_batch(self, batch_event):
        """Process a batch of ticks in the asyncio event loop"""
        try:
            if "batch" not in batch_event or not batch_event["batch"]:
                logger.warning("Empty batch received")
                return
                
            stream_type = batch_event["stream_type"]
            tick_count = len(batch_event["batch"])
            success_count = 0
            
            logger.debug(f"Processing {tick_count} ticks of '{stream_type}' mode")
            
            # Process each tick and add to queue
            for tick in batch_event["batch"]:
                try:
                    # Create an event for the queue
                    event = {
                        "stream_type": stream_type,
                        "data": tick
                    }
                    
                    # Add to queue with a short timeout to avoid blocking
                    try:
                        await asyncio.wait_for(self.queue.put(event), 0.5)
                        success_count += 1
                        self.stats["ticks_processed"] += 1
                    except asyncio.TimeoutError:
                        logger.warning(f"Queue put timeout for {tick.get('trading_symbol', 'unknown')} in {stream_type} mode")
                        self.stats["queue_errors"] += 1
                    except Exception as e:
                        logger.error(f"Error adding to queue: {e}")
                        self.stats["queue_errors"] += 1
                        
                    # Execute callbacks if any
                    for callback in self.callbacks:
                        try:
                            await callback(tick)
                        except Exception as e:
                            logger.error(f"Callback error: {e}")
                            
                except Exception as e:
                    logger.error(f"Error processing tick: {e}")
            
            # Log results
            if success_count > 0:
                logger.info(f"Successfully queued {success_count}/{tick_count} '{stream_type}' ticks (total processed: {self.stats['ticks_processed']})")
            elif tick_count > 0:
                logger.warning(f"Failed to queue any of {tick_count} '{stream_type}' ticks")
                
        except Exception as e:
            logger.error(f"Error in _process_tick_batch: {e}")
            self.stats["processing_errors"] += 1

    def _on_error(self, ws, code, reason):
        """Handle WebSocket errors"""
        error_msg = f"WebSocket error: {code} - {reason}"
        logger.error(error_msg)
        
        # Check for rate limiting errors
        if code == 429 or "429" in str(reason) or "TooManyRequests" in str(reason):
            self._rate_limited = True
            self._rate_limit_start = time.time()
            logger.warning(f"Rate limiting detected. Entering cooldown for {self._rate_limit_cooldown} seconds")

    def _on_close(self, ws, code, reason):
        """Handle WebSocket closure"""
        logger.warning(f"WebSocket closed: {code} - {reason}")

    def _on_reconnect(self, ws, attempts_count):
        """Handle WebSocket reconnection"""
        logger.info(f"Reconnecting attempt {attempts_count}")

    def start(self):
        """Start the connector"""
        self.running = True
        
        # Get the current event loop or create a new one
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            # If there's no event loop, create one
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
        
        # Start the connection task
        self._connection_task = asyncio.create_task(self.connect())
        logger.info("Started Zerodha connector")

    def stop(self):
        """Stop the connector"""
        self.running = False
        if self.kws:
            try:
                self.kws.close()
            except:
                pass
        if self._connection_task:
            self._connection_task.cancel()
        logger.info("Stopped Zerodha connector")
        
        # Log final stats
        logger.info(f"Final stats: received={self.stats['ticks_received']}, "
                   f"processed={self.stats['ticks_processed']}, "
                   f"queue_errors={self.stats['queue_errors']}, "
                   f"processing_errors={self.stats['processing_errors']}")

    async def _connect_websocket(self):
        """Connect to WebSocket with exponential backoff"""
        retry_count = 0
        max_retries = 5
        base_delay = 1  # Base delay in seconds
        
        while retry_count < max_retries:
            try:
                if self.ws:
                    await self.ws.close()
                
                # Add delay before reconnection
                if retry_count > 0:
                    delay = base_delay * (2 ** (retry_count - 1))  # Exponential backoff
                    logger.info(f"Waiting {delay} seconds before reconnection attempt {retry_count + 1}")
                    await asyncio.sleep(delay)
                
                self.ws = await websockets.connect(
                    self.ws_url,
                    extra_headers=self.headers,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=10
                )
                
                # Reset retry count on successful connection
                retry_count = 0
                logger.info("WebSocket connected successfully")
                return True
                
            except Exception as e:
                retry_count += 1
                logger.error(f"WebSocket connection failed (attempt {retry_count}/{max_retries}): {str(e)}")
                
                if retry_count >= max_retries:
                    logger.error("Max retry attempts reached. Giving up.")
                    return False
                
                # If we get a 429 error, increase the delay
                if "429" in str(e):
                    base_delay *= 2
                
                continue
        
        return False

    async def _subscribe(self):
        """Subscribe to instruments with rate limiting"""
        if not self.ws:
            logger.error("WebSocket not connected")
            return False
            
        try:
            # Process instruments in smaller batches
            batch_size = 50  # Reduced from 100
            for i in range(0, len(self.instrument_tokens), batch_size):
                batch = self.instrument_tokens[i:i + batch_size]
                
                # Add delay between batches
                if i > 0:
                    await asyncio.sleep(1)  # 1 second delay between batches
                
                subscribe_message = {
                    "a": "subscribe",
                    "v": [f"{inst['exchange']}:{inst}" for inst in batch]
                }
                
                await self.ws.send(json.dumps(subscribe_message))
                logger.info(f"Subscribed to batch {i//batch_size + 1} of {(len(self.instrument_tokens) + batch_size - 1)//batch_size}")
                
            return True
            
        except Exception as e:
            logger.error(f"Subscription failed: {str(e)}")
            return False