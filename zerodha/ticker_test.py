import asyncio
import time
import os
import logging
from kiteconnect import KiteConnect, KiteTicker
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(ENV_PATH)

# Get API credentials
API_KEY = os.getenv('ZERODHA_API_KEY')
ACCESS_TOKEN = os.getenv('ZERODHA_ACCESS_TOKEN')

if not API_KEY or not ACCESS_TOKEN:
    logger.error("API_KEY or ACCESS_TOKEN not found in .env file")
    exit(1)

# Initialize KiteConnect API client
kite = KiteConnect(api_key=API_KEY)
kite.set_access_token(ACCESS_TOKEN)

tick_count = 0
last_tick_time = time.time()
tick_data = []

def on_ticks(ws, ticks):
    """Callback when ticks are received."""
    global tick_count, last_tick_time, tick_data
    tick_count += len(ticks)
    
    # Store a sample of ticks for debugging
    if len(tick_data) < 10:
        for tick in ticks:
            if len(tick_data) < 10:
                # Create a simplified copy without large data
                simple_tick = {
                    'instrument_token': tick.get('instrument_token'),
                    'tradingsymbol': tick.get('tradingsymbol'),
                    'exchange': tick.get('exchange'),
                    'last_price': tick.get('last_price')
                }
                tick_data.append(simple_tick)
    
    # Print tick rate stats every 5 seconds
    current_time = time.time()
    if current_time - last_tick_time >= 5:
        rate = tick_count / (current_time - last_tick_time)
        logger.info(f"Received {tick_count} ticks at {rate:.2f} ticks/sec")
        logger.info(f"First few ticks: {tick_data[:3]}")
        tick_count = 0
        last_tick_time = current_time

def on_connect(ws, response):
    """Callback when connection is established."""
    logger.info("Connected to ticker")
    # Get some popular instrument tokens to subscribe to
    try:
        # Get Nifty 50 instruments
        nifty = kite.instruments("NSE")
        tokens = [n['instrument_token'] for n in nifty[:50]]  # First 50 instruments
        
        # Subscribe to these tokens
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        logger.info(f"Subscribed to {len(tokens)} instruments")
    except Exception as e:
        logger.error(f"Error subscribing to instruments: {e}")

def on_close(ws, code, reason):
    """Callback when connection is closed."""
    logger.warning(f"Connection closed: {code} - {reason}")

def on_error(ws, code, reason):
    """Callback when connection has an error."""
    logger.error(f"Connection error: {code} - {reason}")

def main():
    """Main function to start the ticker."""
    # Create a ticker instance
    ticker = KiteTicker(API_KEY, ACCESS_TOKEN)
    
    # Set callbacks
    ticker.on_ticks = on_ticks
    ticker.on_connect = on_connect
    ticker.on_close = on_close
    ticker.on_error = on_error
    
    # Connect to ticker
    logger.info("Connecting to ticker...")
    ticker.connect()

    # Keep the script running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping ticker...")
        ticker.close()

if __name__ == "__main__":
    main() 