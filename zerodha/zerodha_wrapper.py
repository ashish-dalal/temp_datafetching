import threading
import time
import pandas as pd
import numpy as np
from kiteconnect import KiteConnect, KiteTicker
from datetime import datetime, timedelta
import logging
from uuid import uuid4

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ZerodhaBrokerWrapper:
    def __init__(self, api_key, api_secret, access_token=None):
        """Initialize the Zerodha broker wrapper."""
        self.api_key = api_key
        self.api_secret = api_secret
        self.kite = KiteConnect(api_key=api_key)
        self.access_token = access_token
        self.ticker = None
        self.symbol = None
        self.timeframe = None
        self.instrument_token = None
        self.data = pd.DataFrame()
        self.position = None
        self.trailing_sl = None
        self.take_profit = None
        self.order_lock = threading.Lock()

        if access_token:
            self.kite.set_access_token(access_token)
        else:
            logger.error("Access token required. Please authenticate manually.")
            raise ValueError("Access token is not provided.")

    def authenticate(self, request_token):
        """Authenticate and generate access token."""
        try:
            data = self.kite.generate_session(request_token, api_secret=self.api_secret)
            self.access_token = data["access_token"]
            self.kite.set_access_token(self.access_token)
            logger.info("Authentication successful. Access token set.")
            return self.access_token
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            raise

    def set_symbol_timeframe(self, symbol, timeframe):
        """Set the trading symbol and timeframe."""
        self.symbol = symbol
        self.timeframe = timeframe
        # Map timeframe to Kite Connect intervals
        timeframe_map = {
            "1minute": "minute",
            "5minute": "5minute",
            "15minute": "15minute",
            "30minute": "30minute",
            "60minute": "60minute",
            "day": "day"
        }
        if timeframe not in timeframe_map:
            logger.error(f"Unsupported timeframe: {timeframe}")
            raise ValueError(f"Unsupported timeframe. Choose from {list(timeframe_map.keys())}")
        self.kite_timeframe = timeframe_map[timeframe]
        
        # Get instrument token
        try:
            instruments = self.kite.instruments("NSE")
            for instrument in instruments:
                if instrument["tradingsymbol"] == symbol:
                    self.instrument_token = instrument["instrument_token"]
                    break
            if not self.instrument_token:
                logger.error(f"Symbol {symbol} not found in NSE instruments.")
                raise ValueError(f"Symbol {symbol} not found.")
            logger.info(f"Set symbol: {symbol}, timeframe: {timeframe}, instrument_token: {self.instrument_token}")
        except Exception as e:
            logger.error(f"Error setting symbol/timeframe: {e}")
            raise

    def fetch_historical_data(self, days=5):
        """Fetch historical data for the symbol."""
        try:
            to_date = datetime.now()
            from_date = to_date - timedelta(days=days)
            data = self.kite.historical_data(
                instrument_token=self.instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval=self.kite_timeframe,
                continuous=False
            )
            self.data = pd.DataFrame(data)
            self.data['date'] = pd.to_datetime(self.data['date'])
            self.data.set_index('date', inplace=True)
            logger.info(f"Fetched {len(self.data)} historical records for {self.symbol}.")
            return self.data
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            raise

    def start_websocket(self):
        """Start WebSocket for live data."""
        if self.ticker and hasattr(self.ticker, 'is_connected') and self.ticker.is_connected():
            logger.warning("WebSocket is already connected. Stopping existing connection first.")
            self.stop()
        
        try:
            logger.info(f"Starting WebSocket connection for {self.symbol} (token: {self.instrument_token})")
            self.ticker = KiteTicker(self.api_key, self.access_token)
            
            # Register callbacks
            self.ticker.on_ticks = self.on_ticks
            self.ticker.on_connect = self.on_connect
            self.ticker.on_close = self.on_close
            self.ticker.on_error = self.on_error
            
            # Set max reconnection attempts
            self.ticker.enable_reconnect(max_retry_attempts=10, max_delay=60)
            
            # Start the connection
            self.ticker.connect(threaded=True)
            logger.info("WebSocket started and attempting connection.")
        except Exception as e:
            logger.error(f"Error starting WebSocket: {e}")
            raise

    def on_connect(self, ws, response):
        """WebSocket on_connect callback."""
        ws.subscribe([self.instrument_token])
        ws.set_mode(ws.MODE_FULL, [self.instrument_token])
        logger.info(f"WebSocket connected. Subscribed to {self.symbol}.")

    def on_close(self, ws, code, reason):
        """WebSocket on_close callback."""
        logger.warning(f"WebSocket connection closed: {code} - {reason}")
        
        # Try to reconnect
        try:
            time.sleep(3)  # Wait a bit before attempting to reconnect
            logger.info("Attempting to reconnect after connection close...")
            self.start_websocket()
        except Exception as e:
            logger.error(f"Failed to reconnect after WebSocket close: {e}")

    def on_ticks(self, ws, ticks):
        """WebSocket on_ticks callback."""
        for tick in ticks:
            if tick['instrument_token'] == self.instrument_token:
                # Create timestamp - use the one from tick if available, otherwise use current time
                tick_time = datetime.now()
                if 'timestamp' in tick:
                    tick_time = pd.to_datetime(tick['timestamp'])
                
                tick_data = {
                    'date': tick_time,
                    'open': tick.get('open', np.nan),
                    'high': tick.get('high', np.nan),
                    'low': tick.get('low', np.nan),
                    'close': tick['last_price'],
                    'volume': tick.get('volume', 0)
                }
                
                # Log the incoming tick data
                logger.debug(f"Received tick: {tick}")
                
                # Append to data frame
                self.data = pd.concat([self.data, pd.DataFrame([tick_data], index=[tick_time])])
                
                # Resample data to maintain proper timeframe
                self.data = self.data.resample(self.timeframe).agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()
                
                # Check trailing stop-loss and take-profit
                self.check_trailing_sl_tp(tick['last_price'])

    def on_error(self, ws, code, reason):
        """WebSocket on_error callback."""
        logger.error(f"WebSocket error: {code} - {reason}")
        
        # Try to reconnect
        try:
            # Wait a bit before reconnecting
            time.sleep(5)
            logger.info("Attempting to reconnect WebSocket...")
            self.start_websocket()
        except Exception as e:
            logger.error(f"Failed to reconnect WebSocket: {e}")
            # If reconnection fails multiple times, might need manual intervention
            # Consider implementing a max_retry counter

    def place_market_order(self, transaction_type, quantity):
        """Place a market order."""
        with self.order_lock:
            try:
                order_id = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=self.kite.EXCHANGE_NSE,
                    tradingsymbol=self.symbol,
                    transaction_type=transaction_type,
                    quantity=quantity,
                    product=self.kite.PRODUCT_CNC,
                    order_type=self.kite.ORDER_TYPE_MARKET
                )
                self.position = {
                    'order_id': order_id,
                    'type': transaction_type,
                    'quantity': quantity,
                    'entry_price': self.data['close'].iloc[-1]
                }
                logger.info(f"Placed market order: {transaction_type} {quantity} {self.symbol}, Order ID: {order_id}")
                return order_id
            except Exception as e:
                logger.error(f"Error placing market order: {e}")
                raise

    def place_limit_order(self, transaction_type, quantity, limit_price):
        """Place a limit order."""
        with self.order_lock:
            try:
                order_id = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=self.kite.EXCHANGE_NSE,
                    tradingsymbol=self.symbol,
                    transaction_type=transaction_type,
                    quantity=quantity,
                    product=self.kite.PRODUCT_CNC,
                    order_type=self.kite.ORDER_TYPE_LIMIT,
                    price=limit_price
                )
                self.position = {
                    'order_id': order_id,
                    'type': transaction_type,
                    'quantity': quantity,
                    'entry_price': limit_price
                }
                logger.info(f"Placed limit order: {transaction_type} {quantity} {self.symbol} at {limit_price}, Order ID: {order_id}")
                return order_id
            except Exception as e:
                logger.error(f"Error placing limit order: {e}")
                raise

    def place_sl_order(self, transaction_type, quantity, trigger_price, limit_price=None):
        """Place a stop-loss order."""
        with self.order_lock:
            try:
                order_type = self.kite.ORDER_TYPE_SL if limit_price else self.kite.ORDER_TYPE_SLM
                order_params = {
                    'variety': self.kite.VARIETY_REGULAR,
                    'exchange': self.kite.EXCHANGE_NSE,
                    'tradingsymbol': self.symbol,
                    'transaction_type': transaction_type,
                    'quantity': quantity,
                    'product': self.kite.PRODUCT_CNC,
                    'order_type': order_type,
                    'trigger_price': trigger_price
                }
                if limit_price:
                    order_params['price'] = limit_price
                order_id = self.kite.place_order(**order_params)
                logger.info(f"Placed SL order: {transaction_type} {quantity} {self.symbol}, Trigger: {trigger_price}, Order ID: {order_id}")
                return order_id
            except Exception as e:
                logger.error(f"Error placing SL order: {e}")
                raise

    def set_take_profit(self, tp_price):
        """Set a take-profit price to monitor."""
        if not self.position:
            logger.error("No active position to set take-profit.")
            raise ValueError("No active position.")
        self.take_profit = tp_price
        logger.info(f"Set take-profit at {tp_price} for {self.symbol}")

    def set_trailing_sl(self, trail_points):
        """Set a trailing stop-loss in points."""
        if not self.position:
            logger.error("No active position to set trailing SL.")
            raise ValueError("No active position.")
        entry_price = self.position['entry_price']
        if self.position['type'] == self.kite.TRANSACTION_TYPE_BUY:
            self.trailing_sl = {
                'trail_points': trail_points,
                'current_sl': entry_price - trail_points,
                'highest_price': entry_price
            }
        else:
            self.trailing_sl = {
                'trail_points': trail_points,
                'current_sl': entry_price + trail_points,
                'lowest_price': entry_price
            }
        logger.info(f"Set trailing SL: {trail_points} points for {self.symbol}")

    def check_trailing_sl_tp(self, current_price):
        """Check and execute trailing SL and TP."""
        if not self.position:
            return
        transaction_type = self.kite.TRANSACTION_TYPE_SELL if self.position['type'] == self.kite.TRANSACTION_TYPE_BUY else self.kite.TRANSACTION_TYPE_BUY

        # Check take-profit
        if self.take_profit:
            if (self.position['type'] == self.kite.TRANSACTION_TYPE_BUY and current_price >= self.take_profit) or \
               (self.position['type'] == self.kite.TRANSACTION_TYPE_SELL and current_price <= self.take_profit):
                try:
                    order_id = self.place_market_order(transaction_type, self.position['quantity'])
                    logger.info(f"Take-profit triggered at {current_price}. Exited position. Order ID: {order_id}")
                    self.position = None
                    self.take_profit = None
                    self.trailing_sl = None
                except Exception as e:
                    logger.error(f"Error executing TP order: {e}")

        # Check trailing stop-loss
        if self.trailing_sl:
            if self.position['type'] == self.kite.TRANSACTION_TYPE_BUY:
                # Update highest price and trailing SL
                self.trailing_sl['highest_price'] = max(self.trailing_sl['highest_price'], current_price)
                new_sl = self.trailing_sl['highest_price'] - self.trailing_sl['trail_points']
                self.trailing_sl['current_sl'] = max(self.trailing_sl['current_sl'], new_sl)
                if current_price <= self.trailing_sl['current_sl']:
                    try:
                        order_id = self.place_market_order(transaction_type, self.position['quantity'])
                        logger.info(f"Trailing SL triggered at {current_price}. Exited position. Order ID: {order_id}")
                        self.position = None
                        self.trailing_sl = None
                        self.take_profit = None
                    except Exception as e:
                        logger.error(f"Error executing trailing SL order: {e}")
            else:
                # Update lowest price and trailing SL
                self.trailing_sl['lowest_price'] = min(self.trailing_sl['lowest_price'], current_price)
                new_sl = self.trailing_sl['lowest_price'] + self.trailing_sl['trail_points']
                self.trailing_sl['current_sl'] = min(self.trailing_sl['current_sl'], new_sl)
                if current_price >= self.trailing_sl['current_sl']:
                    try:
                        order_id = self.place_market_order(transaction_type, self.position['quantity'])
                        logger.info(f"Trailing SL triggered at {current_price}. Exited position. Order ID: {order_id}")
                        self.position = None
                        self.trailing_sl = None
                        self.take_profit = None
                    except Exception as e:
                        logger.error(f"Error executing trailing SL order: {e}")

    def get_current_data(self):
        """Return the latest OHLCV data."""
        if self.data.empty:
            logger.warning("No data available. Fetch historical data or start WebSocket.")
            return None
        return self.data.iloc[-1]

    def stop(self):
        """Stop the WebSocket and cleanup."""
        if self.ticker:
            self.ticker.close()
            logger.info("WebSocket stopped.")
        self.position = None
        self.trailing_sl = None
        self.take_profit = None

# Example usage
if __name__ == "__main__":
    # Replace with your API credentials
    API_KEY = "n3a1ly52x1d99ntm"
    API_SECRET = "kdatn7jgpehykadplml2f91hxhsuk7ht"
    ACCESS_TOKEN = "h3fYFtmZxAsaMFJP0RPfXuNwaipl2Xcn" 

    # Initialize wrapper
    broker = ZerodhaBrokerWrapper(API_KEY, API_SECRET, ACCESS_TOKEN)

    # Set symbol and timeframe
    broker.set_symbol_timeframe("GOLDBEES", "15minute")

    # Fetch historical data
    historical_data = broker.fetch_historical_data(days=5)
    print("Historical Data:\n", historical_data.tail())

    # Start WebSocket for live data
    broker.start_websocket()

    # Place a market order
    order_id = broker.place_market_order(broker.kite.TRANSACTION_TYPE_BUY, quantity=5)
    print(f"Market Order ID: {order_id}")

    # Set take-profit and trailing stop-loss
    current_price = broker.get_current_data()['close']
    broker.set_take_profit(current_price + 50)  # TP at 50 points above entry
    broker.set_trailing_sl(trail_points=20)  # Trail SL by 20 points

    # Simulate running for some time
    time.sleep(300)  # Run for 5 minutes

    # Stop the wrapper
    broker.stop()