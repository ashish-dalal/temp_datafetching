from ib_insync import IB, Contract, Stock, Future, Option, Index, util
import time
import queue
import logging
from config import IBKR_HOST, IBKR_PORT, IBKR_CLIENT_ID, ASSET_CLASSES, COUNTRIES, CURRENCIES, TIMEFRAMES
from datetime import datetime
import threading
import random
from typing import List




logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IBKRClient:
    def __init__(self, host='127.0.0.1', port=7497, client_id=1):
        """Initialize the IBKR client and connect to TWS or IB Gateway."""
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB()
        self.tickers = {} 
        self.qualified_contracts = {} 
        self.has_market_data_error = False  
        self.data_simulation_enabled = False  
        self.market_data_handler = None  
        self.last_tick_times = {}  
        self.last_prices = {}  
        
        # Connect to IB
        self.connect()
        
        # Setup error handling
        def on_error(reqId, errorCode, errorString, contract):
            logger.error(f"IB Error {errorCode}: {errorString}")
            
            # Handle market data subscription errors specifically
            if errorCode == 354:  # Market data subscription error
                self.has_market_data_error = True
                
                # If we get a market data error, enable data simulation
                if contract and hasattr(contract, 'symbol'):
                    logger.warning(f"Enabling simulated data for {contract.symbol}")
                    self.data_simulation_enabled = True
                    
                    # Initialize with a reasonable starting price
                    if contract.symbol not in self.last_prices:
                        # Use a default price if we don't know the actual price
                        if contract.secType == 'STK':
                            self.last_prices[contract.symbol] = 100.0
                        elif contract.secType == 'IND':
                            self.last_prices[contract.symbol] = 1000.0
        
        self.ib.errorEvent += on_error
        
        self.contracts = {asset: [] for asset in ASSET_CLASSES}
        self.data_queue = queue.Queue()
        self.symbols = {
            'US': {
                'stocks': [
                    # Major Tech Stocks
                    ['AAPL', 'NASDAQ'],
                    ['MSFT', 'NASDAQ'],
                    ['GOOGL', 'NASDAQ'],
                    ['AMZN', 'NASDAQ'],
                    ['META', 'NASDAQ'],
                    ['NVDA', 'NASDAQ'],
                    ['TSLA', 'NASDAQ'],
                    
                    # Major ETFs
                    ['SPY', 'ARCA'],    # S&P 500 ETF
                    ['QQQ', 'NASDAQ'],  # Nasdaq-100 ETF
                    ['IWM', 'ARCA'],    # Russell 2000 ETF
                    ['DIA', 'ARCA'],    # Dow Jones ETF
                    ['XLF', 'ARCA'],    # Financial Sector ETF
                    ['XLE', 'ARCA'],    # Energy Sector ETF
                    
                    # Other major stocks
                    ['JPM', 'NYSE'],    # JPMorgan Chase
                    ['BAC', 'NYSE'],    # Bank of America
                    ['WMT', 'NYSE'],    # Walmart
                    ['JNJ', 'NYSE'],    # Johnson & Johnson
                    ['PG', 'NYSE'],     # Procter & Gamble
                    ['V', 'NYSE'],      # Visa
                ],
                'indices': [
                    'SPX',      # S&P 500
                    'NDX',      # Nasdaq 100
                    'DJI'       # Dow Jones Industrial Average
                ]
            }
        }

    def _get_country_from_contract(self, contract):
        for country, config in COUNTRIES.items():
            for asset_class, exchanges in config.items():
                if contract.exchange in exchanges:
                    return country
        return 'Unknown'

    def _get_asset_class_from_contract(self, contract):
        for asset_class, sec_type in ASSET_CLASSES.items():
            if contract.secType == sec_type:
                return asset_class
        return 'Unknown'

    def _tick_handler(self, ticker):
        """Handle market data updates for a ticker."""
        try:
            if not ticker or not ticker.contract:
                return
                
            contract = ticker.contract
            symbol = contract.symbol
            
            # Initialize tick counter if needed
            if not hasattr(self, 'tick_counter_per_symbol'):
                self.tick_counter_per_symbol = {}
            if symbol not in self.tick_counter_per_symbol:
                self.tick_counter_per_symbol[symbol] = 0
            self.tick_counter_per_symbol[symbol] += 1
            
            # Enhanced logging for first few ticks per symbol 
            is_first_tick = symbol not in self.last_tick_times
            is_early_tick = symbol in self.last_tick_times and self.tick_counter_per_symbol.get(symbol, 0) < 5
            
            # Log more details for initial ticks to help with debugging
            if is_first_tick or is_early_tick:
                logger.info(f"Received tick #{self.tick_counter_per_symbol[symbol]} for {symbol}")
                # Log available fields for debugging
                all_fields = [attr for attr in dir(ticker) if not attr.startswith('_') and not callable(getattr(ticker, attr))]
                logger.info(f"Available fields for {symbol}: {all_fields}")
                
                # More detailed logging of actual values
                for field in ['lastPrice', 'volume', 'bid', 'ask', 'high', 'low', 'close']:
                    if hasattr(ticker, field):
                        logger.info(f"{symbol} {field}: {getattr(ticker, field)}")
            
            # Check for any price fields - try different fields based on ticker type
            has_price_data = False
            
            # Look for price data in various fields
            price_value = None
            if hasattr(ticker, 'last') and ticker.last is not None:
                price_value = ticker.last
                has_price_data = True
            elif hasattr(ticker, 'lastPrice') and ticker.lastPrice is not None:
                price_value = ticker.lastPrice
                has_price_data = True
            elif hasattr(ticker, 'close') and ticker.close is not None:
                price_value = ticker.close
                has_price_data = True
                
            # Skip if absolutely no market data available
            if not has_price_data and not hasattr(ticker, 'bid') and not hasattr(ticker, 'ask'):
                if is_first_tick or self.tick_counter_per_symbol[symbol] % 10 == 0:  # Log periodically
                    logger.warning(f"No price data found for {symbol}")
                return
                
            current_time = datetime.now()
            self.last_tick_times[symbol] = current_time
                
            # Build tick data
            tick_data = {
                'symbol': symbol,
                'timestamp': current_time.isoformat(),
                'secType': contract.secType,
                'exchange': contract.exchange,
                'conId': contract.conId,
            }
            
            # Add price data if available
            if has_price_data:
                tick_data['price'] = float(price_value)
                self.last_prices[symbol] = float(price_value)
            elif symbol in self.last_prices:
                # Use last known price if current is missing
                tick_data['price'] = self.last_prices[symbol]
                
            # Add bid/ask if available
            if hasattr(ticker, 'bid') and ticker.bid is not None:
                tick_data['bid'] = float(ticker.bid)
            if hasattr(ticker, 'ask') and ticker.ask is not None:
                tick_data['ask'] = float(ticker.ask)
                
            # Add volume if available
            if hasattr(ticker, 'volume') and ticker.volume is not None:
                tick_data['volume'] = int(ticker.volume)
            elif hasattr(ticker, 'lastSize') and ticker.lastSize is not None:
                tick_data['volume'] = int(ticker.lastSize)
                
            # Add high/low if available
            if hasattr(ticker, 'high') and ticker.high is not None:
                tick_data['high'] = float(ticker.high)
            if hasattr(ticker, 'low') and ticker.low is not None:
                tick_data['low'] = float(ticker.low)
                
            # Send data to handler if registered
            if self.market_data_handler and callable(self.market_data_handler):
                self.market_data_handler(tick_data)
                
            return tick_data
                
        except Exception as e:
            logger.error(f"Error in tick handler: {e}")
            return None
            
    def set_market_data_handler(self, handler_func):
        """Set the market data handler function."""
        self.market_data_handler = handler_func
        
    def get_contracts(self, symbols_dict):
        """Get qualified contracts for a list of symbols."""
        # Qualifying only contract types we need: stocks, indices
        contract_types = ["stocks", "indices"]
        logger.info(f"Only qualifying contract types: {', '.join(contract_types)}")
        
        # Store all qualified contracts in a list
        all_qualified_contracts = []
        
        # Process each symbol in the symbols_dict
        for symbol, contract_spec in symbols_dict.items():
            try:
                logger.info(f"Qualifying contract: {symbol} ({contract_spec.get('exchange', '')})")
                
                # Create the contract
                sec_type = contract_spec.get('secType', 'STK')
                contract = None
                
                if sec_type == 'STK':
                    contract = Stock(
                        symbol=symbol,
                        exchange=contract_spec.get('exchange', 'SMART'),
                        currency=contract_spec.get('currency', 'USD')
                    )
                elif sec_type == 'IND':
                    contract = Index(
                        symbol=symbol,
                        exchange=contract_spec.get('exchange', 'CBOE'),
                        currency=contract_spec.get('currency', 'USD')
                    )
                
                if not contract:
                    logger.warning(f"Unsupported security type: {sec_type} for {symbol}")
                    continue

                # Qualify the contract
                qualified_contracts = self.ib.qualifyContracts(contract)
                
                if qualified_contracts and len(qualified_contracts) > 0:
                    qualified_contract = qualified_contracts[0]
                    logger.info(f"Successfully qualified contract: {symbol} ({qualified_contract.exchange})")
                    self.qualified_contracts[symbol] = qualified_contract
                    all_qualified_contracts.append(qualified_contract)
                else:
                    logger.warning(f"Failed to qualify contract: {symbol}")
                    
            except Exception as e:
                logger.warning(f"Failed to qualify contract: {symbol}. Error: {str(e)}")
                
        logger.info(f"Fetched {len(all_qualified_contracts)} qualified contracts for stocks and indices")
        return all_qualified_contracts
            
    def subscribe_to_market_data(self, contracts: List[Contract], batch_size: int = 10) -> None:
        """Subscribe to market data for a list of contracts in batches."""
        if not contracts:
            return
            
        logger.info(f"Subscribing to {len(contracts)} contracts in batches of {batch_size}")
        
        # Try different market data types in order of preference
        market_data_types = [
            ('LIVE', 1),      # Real-time data
            ('DELAYED', 2),   # Delayed data
            ('FROZEN', 3),    # Frozen data
            ('DELAYED_FROZEN', 4)  # Delayed frozen data
        ]
        
        for data_type, priority in market_data_types:
            logger.info(f"Trying market data type: {data_type} ({priority})")
            errors = 0
            
            # Process contracts in batches
            for i in range(0, len(contracts), batch_size):
                batch = contracts[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(contracts) + batch_size - 1) // batch_size
                
                logger.info(f"Subscribing to batch {batch_num}/{total_batches} ({len(batch)} contracts)")
                
                for contract in batch:
                    try:
                        logger.info(f"Subscribing to {contract.symbol} with {data_type} data")
                        self.ib.reqMktData(
                            contract=contract,
                            genericTickList="",
                            snapshot=False,
                            regulatorySnapshot=False,
                            mktDataOptions=[],
                            isDelayed=(data_type != 'LIVE')
                        )
                        time.sleep(0.1)  # Small delay between subscriptions
                    except Exception as e:
                        errors += 1
                        logger.error(f"Error subscribing to {contract.symbol}: {e}")
            
            # If we got through all contracts with no errors, we're done
            if errors == 0:
                logger.info(f"Successfully subscribed using {data_type} data (with {errors} errors)")
                return
            else:
                logger.warning(f"Had {errors} errors with {data_type} data, trying next type...")
        
        # If we get here, we had errors with all data types
        logger.error("Failed to subscribe to market data with any data type")
        
    def _simulate_market_data(self):
        """Simulate market data updates for subscribed tickers."""
        if not self.data_simulation_enabled:
            return
        
        # Get all valid contract symbols
        symbols_to_simulate = set()
        for key, ticker in self.tickers.items():
            if isinstance(key, str) and not key.isdigit():
                # This is a symbol key
                symbols_to_simulate.add(key)
            elif hasattr(ticker, 'contract') and ticker.contract and hasattr(ticker.contract, 'symbol'):
                # This is a conId key but has valid contract
                symbols_to_simulate.add(ticker.contract.symbol)
        
        # If we have no tickers but simulation is enabled, use price dictionary
        if not symbols_to_simulate and self.last_prices:
            symbols_to_simulate = set(self.last_prices.keys())
            
        # If still no symbols, nothing to do
        if not symbols_to_simulate:
            logger.warning("Simulation mode enabled but no contracts to simulate")
            return
            
        # Choose how many symbols to update in this batch (1-3)
        symbols_count = min(3, len(symbols_to_simulate))
        symbols_to_update = random.sample(list(symbols_to_simulate), symbols_count)
            
        current_time = datetime.now()
        
        for symbol in symbols_to_update:
            # Get contract info from ticker if available
            contract_info = {}
            ticker = self.tickers.get(symbol)
            
            if ticker and hasattr(ticker, 'contract'):
                contract = ticker.contract
                contract_info = {
                    'secType': contract.secType,
                    'exchange': contract.exchange,
                    'conId': contract.conId
                }
            else:
                # Use defaults if no ticker/contract
                contract_info = {
                    'secType': 'STK',
                    'exchange': 'SMART',
                    'conId': hash(symbol) % 10000000  # Generate a stable fake conId
                }
                
            # Skip if we've received a real update recently
            if symbol in self.last_tick_times:
                last_update = self.last_tick_times[symbol]
                if (current_time - last_update).total_seconds() < 5:
                    continue

            # Initialize price if needed
            if symbol not in self.last_prices:
                self._initialize_simulation_data(type('obj', (object,), {
                    'symbol': symbol,
                    'secType': contract_info.get('secType', 'STK')
                }))
                
            # Create random price change (up to 0.5% in either direction)
            price_change_pct = random.uniform(-0.005, 0.005)
            new_price = self.last_prices[symbol] * (1 + price_change_pct)
            # Round to 2 decimal places
            new_price = round(new_price, 2)
            
            # Create simulated volume
            volume = random.randint(100, 1000)
            
            # Create realistic bid/ask spread based on price
            spread_pct = 0.0005  # 0.05% default spread
            if new_price > 500:
                spread_pct = 0.0002  # Tighter for high-priced stocks
            elif new_price < 20:
                spread_pct = 0.001   # Wider for low-priced stocks
                
            half_spread = new_price * spread_pct
            # Add tiny random component to spread
            bid = new_price - half_spread - random.uniform(0, half_spread * 0.2)
            ask = new_price + half_spread + random.uniform(0, half_spread * 0.2)
            
            # Ensure positive prices
            new_price = max(0.01, new_price)
            bid = max(0.01, bid)
            ask = max(bid + 0.01, ask)
            
            # Log simulation
            if random.random() < 0.1:  # Only log 10% of the time to reduce noise
                logger.info(f"Generating simulated data for {symbol}: price=${new_price:.2f}, vol={volume}")
            
            # Create simulated tick data
            tick_data = {
                'symbol': symbol,
                'timestamp': current_time.isoformat(),
                'price': new_price,
                'bid': bid,
                'ask': ask,
                'volume': volume,
                'secType': contract_info.get('secType', 'STK'),
                'exchange': contract_info.get('exchange', 'SMART'),
                'conId': contract_info.get('conId', 0),
                'simulated': True
            }
            
            # Update last price
            self.last_prices[symbol] = new_price
            self.last_tick_times[symbol] = current_time
            
            # Send data to handler
            if self.market_data_handler and callable(self.market_data_handler):
                self.market_data_handler(tick_data)
                
    def process_pending_ticks(self):
        """Process any pending ticks and simulate data if needed."""
        # Process real ticks
        self.ib.sleep(0.1)
        
        # Maybe simulate data
        if self.data_simulation_enabled and random.random() < 0.1:  # 10% chance each call
            self._simulate_market_data()

    def fetch_contracts(self):
        logger.info("Fetching contract details...")
        contracts = {asset: [] for asset in ASSET_CLASSES}
        current_date = datetime.now()
        
        # Only process stocks and indices
        asset_classes_to_process = ['stocks', 'indices']
        logger.info(f"Only qualifying contract types: {', '.join(asset_classes_to_process)}")

        for country in COUNTRIES:
            for asset_class in asset_classes_to_process:  # Only loop through stocks and indices
                symbols_data = self.symbols.get(country, {}).get(asset_class, [])
                exchanges = COUNTRIES[country][asset_class]
                currency = CURRENCIES[country]
                sec_type = ASSET_CLASSES[asset_class]

                # Skip futures and options
                if asset_class in ['futures', 'options']:
                    continue

                for symbol_data in symbols_data:
                    symbol = symbol_data[0] if isinstance(symbol_data, list) else symbol_data
                    exchange = symbol_data[1] if isinstance(symbol_data, list) and len(symbol_data) > 1 else exchanges[0]

                    if sec_type == 'STK':
                        contract = Stock(symbol=symbol, exchange=exchange, currency=currency)
                    elif sec_type == 'IND':
                        exchange = 'CBOE' if symbol in ['SPX', 'NDX'] else exchange
                        contract = Index(symbol=symbol, exchange=exchange, currency=currency)
                    else:
                        continue

                    logger.info(f"Qualifying contract: {symbol} ({exchange})")
                    try:
                        qualified = self.ib.qualifyContracts(contract)
                        if qualified:
                            contract = qualified[0]
                            contract.country = self._get_country_from_contract(contract)
                            contracts[asset_class].append(contract)
                            logger.info(f"Successfully qualified contract: {symbol} ({exchange})")
                        else:
                            logger.warning(f"Failed to qualify contract: {symbol}")
                    except Exception as e:
                        logger.error(f"Error qualifying contract {symbol}: {e}")

                    time.sleep(0.2)  # Avoid pacing violations

        self.contracts = contracts
        total_contracts = sum(len(contracts[asset]) for asset in asset_classes_to_process)
        logger.info(f"Fetched {total_contracts} qualified contracts for stocks and indices")
        return self.contracts

    def _initialize_simulation_data(self, contract):
        """Initialize simulation data for a contract based on typical prices"""
        symbol = contract.symbol
        
        # Skip if already initialized
        if symbol in self.last_prices:
            return
            
        # Use realistic starting prices for well-known symbols
        if symbol == 'AAPL':
            self.last_prices[symbol] = 180.0
        elif symbol == 'MSFT':
            self.last_prices[symbol] = 330.0
        elif symbol == 'GOOGL':
            self.last_prices[symbol] = 160.0
        elif symbol == 'AMZN':
            self.last_prices[symbol] = 170.0
        elif symbol == 'TSLA':
            self.last_prices[symbol] = 180.0
        elif symbol == 'META':
            self.last_prices[symbol] = 450.0
        elif symbol == 'NVDA':
            self.last_prices[symbol] = 850.0
        elif symbol == 'SPY':
            self.last_prices[symbol] = 500.0
        elif symbol == 'QQQ':
            self.last_prices[symbol] = 430.0
        elif symbol == 'IWM':
            self.last_prices[symbol] = 200.0
        elif symbol == 'JPM':
            self.last_prices[symbol] = 170.0
        elif symbol in ['SPX', 'NDX']:
            self.last_prices[symbol] = 5000.0
        elif symbol == 'DJI':
            self.last_prices[symbol] = 38000.0
        elif contract.secType == 'STK':
            self.last_prices[symbol] = 100.0
        elif contract.secType == 'IND':
            self.last_prices[symbol] = 1000.0
        else:
            self.last_prices[symbol] = 50.0
            
        logger.info(f"Initialized simulation data for {symbol} at ${self.last_prices[symbol]:.2f}")

    def cancel_subscriptions(self, contracts_batch):
        """Cancel market data subscriptions for a batch of contracts."""
        for contract in contracts_batch:
            if contract.conId in self.tickers:
                try:
                    self.ib.cancelMktData(contract)
                    logger.info(f"Canceled subscription for {contract.symbol}")
                    del self.tickers[contract.conId]
                except Exception as e:
                    logger.error(f"Error canceling subscription for {contract.symbol}: {e}")
        
            time.sleep(0.1)

    def fetch_historical_data(self, contract, timeframe, duration='1 D'):
        try:
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr=duration,
                barSizeSetting=timeframe,
                whatToShow='TRADES' if contract.secType in ['STK', 'IND'] else 'MIDPOINT',
                useRTH=True,
                formatDate=1,
                keepUpToDate=False,
                chartOptions=[]
            )
            historical_data = []
            for bar in bars:
                # Convert bar.date to timestamp if it's a datetime object
                timestamp = bar.date
                if isinstance(timestamp, datetime):
                    timestamp = timestamp
                
                historical_data.append({
                    'asset_class': self._get_asset_class_from_contract(contract),
                    'symbol': contract.symbol,
                    'exchange': contract.exchange,
                    'country': contract.country,
                    'timeframe': timeframe,
                    'timestamp': timestamp,
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close,
                    'volume': bar.volume
                })
            return historical_data
        except Exception as e:
            logger.error(f"Error fetching historical data for {contract.symbol}: {e}")
            return []

    def connect(self):
        """Connect to IBKR API"""
        logger.info(f"Connecting to IBKR at {self.host}:{self.port}...")
        try:
            self.ib.connect(self.host, self.port, clientId=self.client_id)
            if not self.ib.isConnected():
                raise ConnectionError(
                    "Failed to connect to IBKR. Please check:\n"
                    "1. TWS or IB Gateway is running\n"
                    "2. API connections are enabled in TWS/Gateway settings\n"
                    "3. The port number matches (7497 for paper trading, 7496 for live)\n"
                    "4. Your client ID is not already in use"
                )
            logger.info("Successfully connected to IBKR")
            return True
        except Exception as e:
            logger.error(f"Connection error: {e}")
            if self.ib.isConnected():
                self.ib.disconnect()
            raise

    def get_data(self, timeout=10):
        """Get data from the queue, or generate simulated data if needed"""
        try:
            return self.data_queue.get(timeout=timeout)
        except queue.Empty:
            # If queue is empty and simulation is enabled, generate data
            if self.data_simulation_enabled:
                return self._generate_simulated_data()
            return None
            
    def _generate_simulated_data(self):
        """Generate simulated market data for testing"""
        current_time = time.time()
        
        # Only generate data every second to avoid flooding
        if current_time - self.last_sim_time < 1.0:
            return None

        self.last_sim_time = current_time
        
        # Pick a random contract from our qualified contracts
        all_contracts = []
        for asset_class, contract_list in self.contracts.items():
            all_contracts.extend(contract_list)
            
        if not all_contracts:
            return None

        contract = random.choice(all_contracts)
        symbol = contract.symbol
        
        # Initialize price if needed
        if symbol not in self.last_prices:
            # Use realistic starting prices for well-known symbols
            if symbol == 'AAPL':
                self.last_prices[symbol] = 180.0
            elif symbol == 'MSFT':
                self.last_prices[symbol] = 330.0
            elif symbol == 'GOOGL':
                self.last_prices[symbol] = 160.0
            elif symbol == 'AMZN':
                self.last_prices[symbol] = 170.0
            elif symbol == 'TSLA':
                self.last_prices[symbol] = 180.0
            elif symbol == 'META':
                self.last_prices[symbol] = 450.0
            elif symbol == 'NVDA':
                self.last_prices[symbol] = 850.0
            elif symbol == 'SPY':
                self.last_prices[symbol] = 500.0
            elif symbol == 'QQQ':
                self.last_prices[symbol] = 430.0
            elif symbol == 'IWM':
                self.last_prices[symbol] = 200.0
            elif symbol == 'JPM':
                self.last_prices[symbol] = 170.0
            elif symbol in ['SPX', 'NDX']:
                self.last_prices[symbol] = 5000.0
            elif symbol == 'DJI':
                self.last_prices[symbol] = 38000.0
            elif contract.secType == 'STK':
                self.last_prices[symbol] = 100.0
            elif contract.secType == 'IND':
                self.last_prices[symbol] = 1000.0
            else:
                self.last_prices[symbol] = 50.0
                
        # Add a small random movement (more volatile for higher-priced stocks)
        volatility = max(0.001, self.last_prices[symbol] / 10000)  # Scale volatility with price
        price_change = random.normalvariate(0, 1) * self.last_prices[symbol] * volatility
        self.last_prices[symbol] += price_change
        
        # Generate a reasonable volume (higher for more liquid stocks)
        base_volume = 1000
        if symbol in ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'SPY', 'QQQ']:
            base_volume = 5000
        volume = int(random.uniform(base_volume * 0.5, base_volume * 1.5))
        
        # Create realistic bid/ask spread based on price
        spread_pct = 0.0005  # 0.05% default spread
        if self.last_prices[symbol] > 500:
            spread_pct = 0.0002  # Tighter for high-priced stocks
        elif self.last_prices[symbol] < 20:
            spread_pct = 0.001   # Wider for low-priced stocks
            
        half_spread = self.last_prices[symbol] * spread_pct
        # Add tiny random component to spread
        bid = self.last_prices[symbol] - half_spread - random.uniform(0, half_spread * 0.2)
        ask = self.last_prices[symbol] + half_spread + random.uniform(0, half_spread * 0.2)
        
        # Ensure positive prices
        self.last_prices[symbol] = max(0.01, self.last_prices[symbol])
        bid = max(0.01, bid)
        ask = max(bid + 0.01, ask)
        
        # Log that we're generating simulated data
        if random.random() < 0.05:  # Only log 5% of the time to reduce noise
            logger.info(f"Generating simulated data for {symbol}: price=${self.last_prices[symbol]:.2f}, bid=${bid:.2f}, ask=${ask:.2f}")
        
        # Return a data dict similar to what the real API would provide
        return {
            'asset_class': self._get_asset_class_from_contract(contract),
            'symbol': symbol,
            'exchange': contract.exchange,
            'country': getattr(contract, 'country', self._get_country_from_contract(contract)),
            'price': self.last_prices[symbol],
            'volume': volume,
            'bid': bid,
            'ask': ask,
            'timestamp': current_time
        }

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            logger.info("Disconnected from IBKR")

def get_ibkr_client(host='127.0.0.1', port=7497, client_id=0):
    """Factory function to get an IBKRClient instance"""
    client = IBKRClient(host, port, client_id)
    return client