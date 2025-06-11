import asyncio
import logging
import json
import time
from datetime import datetime
from typing import Dict, Any, List
from ib_insync import IB, Stock, Index, util
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ContractInfo:
    """Contract information for IBKR instruments"""
    symbol: str
    conId: int
    secType: str
    exchange: str
    currency: str
    contract: Any = None
    ticker: Any = None

class IBKRConnector:
    """IBKR real-time data connector using ib_insync"""
    
    def __init__(self, queue: asyncio.Queue, symbols_dict: Dict, host: str, port: int, client_id: int):
        self.queue = queue
        self.symbols_dict = symbols_dict
        self.host = host
        self.port = port
        self.client_id = client_id
        
        # IBKR client
        self.ib = IB()
        self.connected = False
        self.contracts = []
        self.running = False
        
        # Metrics
        self.tick_count = 0
        self.last_metrics_update = time.time()
        
        # Data aggregation for candlesticks
        self.candlestick_data = {}
        self.timeframes = {
            '1min': 60,
            '5min': 300,
            '15min': 900,
            '1hour': 3600
        }
    
    async def start(self):
        """Start the IBKR connector"""
        try:
            # Connect to IBKR Gateway
            await self._connect_ibkr()
            
            # Get contracts for symbols
            await self._get_contracts()
            
            # Subscribe to market data
            await self._subscribe_market_data()
            
            # Start data processing
            self.running = True
            await self._run_event_loop()
            
        except Exception as e:
            logger.error(f"Error starting IBKR connector: {e}")
            raise
    
    async def stop(self):
        """Stop the IBKR connector"""
        self.running = False
        if self.connected:
            self.ib.disconnect()
            logger.info("Disconnected from IBKR")
    
    async def _connect_ibkr(self):
        """Connect to IBKR Gateway/TWS"""
        try:
            await self.ib.connectAsync(
                host=self.host,
                port=self.port,
                clientId=self.client_id,
                timeout=30
            )
            self.connected = True
            logger.info(f"Connected to IBKR at {self.host}:{self.port} (client_id: {self.client_id})")
        except Exception as e:
            logger.error(f"Failed to connect to IBKR: {e}")
            raise
    
    async def _get_contracts(self):
        """Get contract objects for all symbols"""
        logger.info("Fetching contracts...")
        
        for symbol_key, symbol_info in self.symbols_dict.items():  # Iterate over key-value pairs
            try:
                symbol = symbol_info.get('SYMBOL', '')
                sec_type = symbol_info.get('secType', 'STK')
                exchange = symbol_info.get('EXCHANGE', 'SMART')
                currency = symbol_info.get('CURRENCY', 'USD')
                
                # Create contract based on security type
                if sec_type == 'STK':
                    contract = Stock(symbol, exchange, currency)
                elif sec_type == 'IND':
                    contract = Index(symbol, exchange, currency)
                else:
                    logger.warning(f"Unsupported secType: {sec_type} for {symbol}")
                    continue
                
                # Qualify the contract
                qualified_contracts = await self.ib.qualifyContractsAsync(contract)
                
                if qualified_contracts:
                    qualified_contract = qualified_contracts[0]
                    
                    contract_info = ContractInfo(
                        symbol=symbol,
                        conId=qualified_contract.conId,
                        secType=sec_type,
                        exchange=qualified_contract.exchange,
                        currency=currency,
                        contract=qualified_contract
                    )
                    
                    self.contracts.append(contract_info)
                    logger.debug(f"Added contract: {symbol} ({qualified_contract.conId})")
                else:
                    logger.warning(f"Could not qualify contract for {symbol}")
                    
            except Exception as e:
                logger.error(f"Error processing symbol {symbol}: {e}")
        
        logger.info(f"Successfully loaded {len(self.contracts)} contracts")
    
    async def _subscribe_market_data(self):
        """Subscribe to real-time market data for all contracts"""
        logger.info("Subscribing to market data...")
        
        # To set delayed data
        # self.ib.reqMarketDataType(3)  # 3 = delayed data, 1 = real-time data
        
        for contract_info in self.contracts:
            try:
                # Request market data and get ticker object
                ticker = self.ib.reqMktData(
                    contract_info.contract,
                    genericTickList='',
                    snapshot=False,
                    regulatorySnapshot=False
                )
                
                # Store ticker in contract_info for later cleanup
                contract_info.ticker = ticker
                
                # Set event handler on TICKER object (not contract)
                ticker.updateEvent += self._on_tick_update # type:ignore
                
                logger.debug(f"Subscribed to market data for {contract_info.symbol}")
                
            except Exception as e:
                logger.error(f"Error subscribing to {contract_info.symbol}: {e}")
    
    def _on_tick_update(self, ticker):
        """Handle incoming tick data"""
        try:
            # Find contract info using ticker.contract
            contract_info = None
            for c in self.contracts:
                if c.contract.conId == ticker.contract.conId:
                    contract_info = c
                    break
            
            if not contract_info:
                return
            
            # Process tick data
            tick_data = self._process_tick(contract_info, ticker)
            if tick_data:
                # Add to queue asynchronously
                asyncio.create_task(self._queue_data(tick_data))
                
                # Update aggregated data for candlesticks
                self._update_candlestick_data(contract_info, tick_data)
                
                # Update metrics
                self.tick_count += 1
                if time.time() - self.last_metrics_update > 10:  # Update every 10 seconds
                    self._update_metrics()
                    
        except Exception as e:
            logger.error(f"Error processing tick: {e}")
    
    def _process_tick(self, contract_info: ContractInfo, tick) -> Dict[str, Any]:
        """Process raw tick data into standardized format"""
        try:
            # Get current prices
            price = getattr(tick, 'last', None) or getattr(tick, 'close', None)
            bid = getattr(tick, 'bid', None)
            ask = getattr(tick, 'ask', None)
            volume = getattr(tick, 'volume', None)
            
            if price is None:
                return None # type:ignore
            
            return {
                'data_type': 'tick',
                'symbol': contract_info.symbol,
                'conId': contract_info.conId,
                'secType': contract_info.secType,
                'exchange': contract_info.exchange,
                'timestamp': datetime.now().isoformat(),
                'price': float(price),
                'bid': float(bid) if bid is not None else 0.0,
                'ask': float(ask) if ask is not None else 0.0,
                'volume': int(volume) if volume is not None else 0
            }
            
        except Exception as e:
            logger.error(f"Error processing tick for {contract_info.symbol}: {e}")
            return None # type:ignore
    
    def _update_candlestick_data(self, contract_info: ContractInfo, tick_data: Dict[str, Any]):
        """Update candlestick aggregation data"""
        symbol = contract_info.symbol
        price = tick_data['price']
        volume = tick_data['volume']
        current_time = time.time()
        
        if symbol not in self.candlestick_data:
            self.candlestick_data[symbol] = {}
        
        for timeframe, duration in self.timeframes.items():
            # Calculate timeframe bucket
            bucket_start = int(current_time // duration) * duration
            
            if timeframe not in self.candlestick_data[symbol]:
                self.candlestick_data[symbol][timeframe] = {}
            
            if bucket_start not in self.candlestick_data[symbol][timeframe]:
                # New candle
                self.candlestick_data[symbol][timeframe][bucket_start] = {
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume,
                    'timestamp': datetime.fromtimestamp(bucket_start).isoformat()
                }
            else:
                # Update existing candle
                candle = self.candlestick_data[symbol][timeframe][bucket_start]
                candle['high'] = max(candle['high'], price)
                candle['low'] = min(candle['low'], price)
                candle['close'] = price
                candle['volume'] += volume
    
    async def _queue_data(self, data: Dict[str, Any]):
        """Add data to processing queue"""
        try:
            await asyncio.wait_for(self.queue.put(data), timeout=0.1)
        except asyncio.TimeoutError:
            logger.warning("Queue is full, dropping tick data")
        except Exception as e:
            logger.error(f"Error queuing data: {e}")
    
    async def _run_event_loop(self):
        """Main event loop for IBKR connector"""
        logger.info("Starting IBKR event loop...")
        
        # Periodically generate candlestick data
        candlestick_task = asyncio.create_task(self._candlestick_generator())
        
        try:
            while self.running:
                # Process IBKR events
                await asyncio.sleep(0.1)  # Small delay to prevent busy waiting
                
                # IB client runs its own event loop, so we just need to stay alive
                if not self.ib.isConnected():
                    logger.error("Lost connection to IBKR")
                    break
                    
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            candlestick_task.cancel()
    
    async def _candlestick_generator(self):
        """Generate completed candlestick data periodically"""
        while self.running:
            try:
                current_time = time.time()
                
                for symbol, timeframes in self.candlestick_data.items():
                    for timeframe, duration in self.timeframes.items():
                        if timeframe in timeframes:
                            buckets_to_remove = []
                            
                            for bucket_start, candle in timeframes[timeframe].items():
                                # Check if candle is complete (older than current bucket)
                                if current_time >= bucket_start + duration:
                                    # Find contract info for this symbol
                                    contract_info = None
                                    for c in self.contracts:
                                        if c.symbol == symbol:
                                            contract_info = c
                                            break
                                    
                                    if contract_info:
                                        candlestick_data = {
                                            'data_type': 'candlestick',
                                            'symbol': symbol,
                                            'secType': contract_info.secType,
                                            'timeframe': timeframe,
                                            'timestamp': candle['timestamp'],
                                            'open': candle['open'],
                                            'high': candle['high'],
                                            'low': candle['low'],
                                            'close': candle['close'],
                                            'volume': candle['volume']
                                        }
                                        
                                        await self._queue_data(candlestick_data)
                                        buckets_to_remove.append(bucket_start)
                            
                            # Remove completed candles
                            for bucket in buckets_to_remove:
                                del timeframes[timeframe][bucket]
                
                # Wait before next check
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in candlestick generator: {e}")
                await asyncio.sleep(10)
    
    def _update_metrics(self):
        """Update performance metrics"""
        try:
            from run_connector import update_metrics
            update_metrics('ibkr_ticks', self.tick_count, self.queue.qsize())
            self.last_metrics_update = time.time()
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")