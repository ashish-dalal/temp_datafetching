import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from zerodha_connector import ZerodhaConnector
from qdb import QuestDBClient, db_writer, print_full_tick, print_quote_tick, print_ltp_tick
import time
from kiteconnect import KiteConnect
from dotenv import load_dotenv
from typing import List, Dict
from utils import update_metrics, save_metrics, check_metrics_file




# Setup logging and environment
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'

if not os.path.exists(ENV_PATH):
    print(f"ERROR: .env file not found at {ENV_PATH}")
    sys.exit(1)

load_dotenv(ENV_PATH)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

METRICS_FILE = "metrics.json"
BATCH_SIZE = 1000  # Maximum instruments per WebSocket connection
MAX_WEBSOCKETS = 3  # Zerodha allows up to 3 WebSocket connections per app

def load_config() -> Dict:
    """Load configuration from environment variables and config file"""
    api_key = os.getenv('ZERODHA_API_KEY')
    access_token = os.getenv('ZERODHA_ACCESS_TOKEN')
    
    missing = []
    if not api_key:
        missing.append("ZERODHA_API_KEY")
    if not access_token:
        missing.append("ZERODHA_ACCESS_TOKEN")
    
    if missing:
        error_msg = f"Missing required credentials in .env file: {', '.join(missing)}\n"
        error_msg += "Please run the token automation to generate these credentials."
        raise ValueError(error_msg)
    
    # Define all modes
    modes = ['full', 'quote', 'ltp']
    
    config_dict = {
        'zerodha': {
            'api_key': api_key,
            'access_token': access_token,
            'modes': modes  # List of all modes
        },
        'questdb': {
            'host': os.getenv('QUESTDB_HOST', 'localhost'),
            'port': int(os.getenv('QUESTDB_PORT', '8812')),
            'username': os.getenv('QUESTDB_USERNAME', 'admin'),
            'password': os.getenv('QUESTDB_PASSWORD', 'quest')
        }
    }
    return config_dict

def load_instruments() -> List[Dict]:
    """Load and filter instruments from Zerodha"""
    api_key = os.getenv('ZERODHA_API_KEY')
    access_token = os.getenv('ZERODHA_ACCESS_TOKEN')
    
    if not api_key or not access_token:
        raise ValueError("ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN must be set in .env file")
    
    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        all_instruments = kite.instruments()
        
        filtered_instruments = [
            inst for inst in all_instruments
            if inst['exchange'] in ['NSE', 'BSE', 'NFO', 'CDS', 'MCX']
            and inst['segment'] in ['NSE', 'BSE', 'NFO-FUT', 'CDS-FUT', 'MCX-FUT']
        ]
        logger.info(f"Total instruments: {len(filtered_instruments)}")
        return filtered_instruments
    except Exception as e:
        logger.error(f"Error loading instruments: {e}")
        raise

def split_instruments(instruments: List[Dict], batch_size: int = BATCH_SIZE) -> List[List[Dict]]:
    """Split instruments into batches respecting WebSocket limits"""
    return [instruments[i:i + batch_size] for i in range(0, len(instruments), batch_size)]

async def main():
    connectors = []
    db_writer_task = None
    questdb = None
    
    try:
        loop = asyncio.get_event_loop()
        logger.info("Using existing asyncio event loop")
        
        # Load configuration
        config_dict = load_config()
        modes = config_dict['zerodha']['modes']
        
        # Initialize QuestDB connection
        questdb = QuestDBClient()
        if not await questdb.connect():
            logger.error("Failed to connect to QuestDB")
            return
        
        # Create queue with larger size to handle multiple streams
        max_queue_size = 30000  # Increased to handle multiple modes
        data_queue = asyncio.Queue(maxsize=max_queue_size)
        logger.info(f"Created data queue with max size of {max_queue_size} items")
        
        # Start database writer task
        batch_size = 200
        batch_timeout = 0.5
        db_writer_task = asyncio.create_task(db_writer(data_queue, questdb, batch_size, batch_timeout))
        logger.info(f"Database writer task started with batch_size={batch_size}, timeout={batch_timeout}s")
        
        # Load instruments
        instruments = load_instruments()
        logger.info(f"Total instruments loaded: {len(instruments)}")
        
        # Split instruments into batches
        instrument_groups = split_instruments(instruments)
        logger.info(f"Created {len(instrument_groups)} instrument groups")
        
        # Create connectors for each mode and instrument group
        for i, group in enumerate(instrument_groups):
            try:
                logger.info(f"Creating connectors for group {i+1} with {len(group)} instruments")
                
                # Create instrument_mapping dictionary
                instrument_mapping = {}
                for inst in group:
                    instrument_mapping[inst['instrument_token']] = {
                        'exchange': inst['exchange'],
                        'trading_symbol': inst['tradingsymbol']
                    }
                
                instrument_tokens = [inst['instrument_token'] for inst in group]
                
                # Create connector for 'full' mode
                full_connector = ZerodhaConnector(
                    queue=data_queue,
                    instrument_tokens=instrument_tokens,
                    instrument_mapping=instrument_mapping,
                    api_key=config_dict['zerodha']['api_key'],
                    access_token=config_dict['zerodha']['access_token'],
                    mode="full"
                )
                connectors.append(full_connector)
                logger.info(f"Starting 'full' mode connector for group {i+1}")
                full_connector.start()
                
                # Add delay between connector starts
                await asyncio.sleep(10)  # 10 seconds delay between mode connectors
                
                # Create connector for 'quote' mode
                quote_connector = ZerodhaConnector(
                    queue=data_queue,
                    instrument_tokens=instrument_tokens,
                    instrument_mapping=instrument_mapping,
                    api_key=config_dict['zerodha']['api_key'],
                    access_token=config_dict['zerodha']['access_token'],
                    mode="quote"
                )
                connectors.append(quote_connector)
                logger.info(f"Starting 'quote' mode connector for group {i+1}")
                quote_connector.start()
                
                # Add delay between connector starts
                await asyncio.sleep(10)  # 10 seconds delay between mode connectors
                
                # Create connector for 'ltp' mode
                ltp_connector = ZerodhaConnector(
                    queue=data_queue,
                    instrument_tokens=instrument_tokens,
                    instrument_mapping=instrument_mapping,
                    api_key=config_dict['zerodha']['api_key'],
                    access_token=config_dict['zerodha']['access_token'],
                    mode="ltp"
                )
                connectors.append(ltp_connector)
                logger.info(f"Starting 'ltp' mode connector for group {i+1}")
                ltp_connector.start()
                
                if i < len(instrument_groups) - 1:
                    delay = 60  # 1 minute delay between instrument groups
                    logger.info(f"Waiting {delay} seconds before starting next instrument group")
                    await asyncio.sleep(delay)
            except Exception as e:
                logger.error(f"Failed to create or start connectors for group {i+1}: {e}")
                continue
        
        if not connectors:
            logger.error("No connectors could be started. Exiting.")
            return
        
        logger.info(f"Successfully started {len(connectors)} connectors")
        
        # Monitor queue activity
        async def monitor_queue_activity():
            previous_queue_size = 0
            previous_time = time.time()
            
            while True:
                await asyncio.sleep(30)
                try:
                    current_size = data_queue.qsize()
                    current_time = time.time()
                    elapsed = current_time - previous_time
                    rate = (current_size - previous_queue_size) / elapsed if elapsed > 0 else 0
                    logger.info(f"Queue monitor: size={current_size}, change_rate={rate:.2f} items/sec")
                    previous_queue_size = current_size
                    previous_time = current_time
                except Exception as e:
                    logger.error(f"Error in queue monitor: {e}")
        
        monitor_task = asyncio.create_task(monitor_queue_activity())
        
        # Keep running and log stats
        while True:
            await asyncio.sleep(60)
            queue_size = data_queue.qsize()
            logger.info(f"Main loop check: Queue size: {queue_size}")
            
            if int(time.time()) % 300 < 60:
                logger.info("Performing detailed system check...")
                for i, conn in enumerate(connectors):
                    if conn.kws and conn.kws.is_connected():
                        logger.info(f"Connector {i+1} ({conn.mode}): Connected")
                    else:
                        logger.warning(f"Connector {i+1} ({conn.mode}): Not connected")
                if questdb.connected:
                    logger.info("QuestDB: Connected")
                else:
                    logger.warning("QuestDB: Not connected")
                    await questdb.connect()
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    finally:
        if connectors:
            logger.info("Stopping connectors...")
            for connector in connectors:
                connector.stop()
        if db_writer_task:
            logger.info("Stopping database writer...")
            db_writer_task.cancel()
            try:
                await db_writer_task
            except asyncio.CancelledError:
                pass
        if 'monitor_task' in locals() and monitor_task:
            logger.info("Stopping monitoring task...")
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
        if questdb:
            logger.info("Closing QuestDB connection...")
            await questdb.close()
        save_metrics()
        logger.info("Metrics saved to metrics.json")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        logger.info("Starting Zerodha data connector...")
        kite = KiteConnect(api_key=os.getenv('ZERODHA_API_KEY'))
        kite.set_access_token(os.getenv('ZERODHA_ACCESS_TOKEN'))
        profile = kite.profile()
        logger.info(f"Connected as: {profile['user_name']} ({profile['user_id']})") # type:ignore
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)