import time
import logging
import json
import os
from datetime import datetime
import re
import json
from pathlib import Path
from dotenv import load_dotenv
from ibkr_client import get_ibkr_client
from data_processor import process_data
from qdb_writer import get_questdb_writer
from data_aggregator import DataAggregator

ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(ENV_PATH)
QDB_HOST = os.getenv('QUESTDB_HOST')
QDB_PORT = os.getenv('QUESTDB_PORT')
QDB_USER = os.getenv('QUESTDB_USER')
QDB_PASSWORD = os.getenv('QUESTDB_PASSWORD')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Remove timestamp for cleaner console output
console = logging.StreamHandler()
console.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(message)s'))
logging.getLogger('').handlers = [console]

logger = logging.getLogger(__name__)

def main():
    """Main function to run the data fetcher."""
    logger.info("Starting IBKR data fetcher for US-listed stocks and ETFs...")
    
    # Create tables in QuestDB
    questdb_url = os.environ.get('QUESTDB_URL', f'https://{QDB_HOST}:{QDB_PORT}')
    
    # Add retry logic for QuestDB connection
    questdb_connected = False
    max_retries = 3  # Increase retries 
    retry_count = 0
    
    while not questdb_connected and retry_count < max_retries:
        try:
            import create_tables
            tables = create_tables.create_tables(questdb_url)
            questdb_connected = True
            logger.info("Successfully connected to QuestDB and created/verified tables")
        except Exception as e:
            retry_count += 1
            logger.error(f"Error connecting to QuestDB (attempt {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                logger.info(f"Retrying QuestDB connection in 3 seconds...")
                time.sleep(3)
            else:
                logger.error(f"Failed to connect to QuestDB after {max_retries} attempts.")
                logger.error(f"Please check QuestDB server availability at {questdb_url}")
                raise Exception(f"Cannot continue without QuestDB connection at {questdb_url}")
    
    # Connect to QuestDB for real-time data
    try:
        import qdb_writer as qdb
        qdb_writer = qdb.get_questdb_writer(questdb_url, QDB_USER, QDB_PASSWORD)
        logger.info("Connected to QuestDB for direct writes")
    except Exception as e:
        logger.error(f"Error connecting to QuestDB writer: {e}")
        raise Exception(f"Cannot continue without QuestDB connection")
    
    # Initialize data aggregator
    data_aggregator = DataAggregator()
    
    # Configure IBKR connection
    ibkr_host = os.environ.get('IBKR_HOST', '127.0.0.1')
    ibkr_port = int(os.environ.get('IBKR_PORT', 7497))
    client_id = int(os.environ.get('IBKR_CLIENT_ID', 0))
    
    # Dictionary of symbols and their contract specifications
    with open('IBKR_TICKERS.json', 'r') as f:
        symbols_dict = json.load(f)
    
    # Connect to IBKR
    try:
        # Get IBKR client and connect
        ibkr_client = get_ibkr_client(ibkr_host, ibkr_port, client_id)
        
        # Get contracts from IBKR
        all_contracts = ibkr_client.get_contracts(symbols_dict)
        
        # Filter contracts by type
        stock_contracts = [c for c in all_contracts if c.secType == 'STK']
        future_contracts = [c for c in all_contracts if c.secType == 'FUT']
        option_contracts = [c for c in all_contracts if c.secType == 'OPT']
        index_contracts = [c for c in all_contracts if c.secType == 'IND']
        
        # Log contract information
        logger.info(f"Found {len(stock_contracts)} stock contracts")
        if stock_contracts:
            logger.info(f"Sample stocks: {', '.join([c.symbol for c in stock_contracts[:5]])}...")
            
        if future_contracts:
            logger.info(f"Found {len(future_contracts)} future contracts")
            logger.info(f"Futures: {', '.join([c.symbol for c in future_contracts[:5]])}")
            
        if option_contracts:
            logger.info(f"Found {len(option_contracts)} option contracts")
            logger.info(f"Options: {', '.join([f'{c.symbol} {c.strike}' for c in option_contracts[:5]])}")
            
        if index_contracts:
            logger.info(f"Found {len(index_contracts)} index contracts")
            logger.info(f"Indices: {', '.join([c.symbol for c in index_contracts])}")
        
        # Combine all contracts for processing
        all_contracts_to_process = stock_contracts + index_contracts  # Only stocks and indices for now
        logger.info(f"Total contracts to process: {len(all_contracts_to_process)}")
        
        # Initialize stats and timing
        start_time = time.time()
        last_stats_time = start_time
        last_data_time = start_time
        tick_count = 0
        tick_counter_per_symbol = {}
        
        # Set up data handler to track market data
        def handle_market_data(tick_data):
            nonlocal tick_count, last_data_time
            
            tick_count += 1
            last_data_time = time.time()
            
            # Track ticks per symbol
            symbol = tick_data.get('symbol', 'UNKNOWN')
            if symbol not in tick_counter_per_symbol:
                tick_counter_per_symbol[symbol] = 0
            tick_counter_per_symbol[symbol] += 1
            
            # Log every 10 ticks for active symbols
            if tick_counter_per_symbol[symbol] % 10 == 0:
                logger.info(f"{symbol}: Processed {tick_counter_per_symbol[symbol]} ticks")
            
            # Process tick data through aggregator
            completed_timeframes = data_aggregator.process_tick(tick_data)
            
            # Push tick data to QuestDB
            try:
                qdb_writer.insert_tick(tick_data)
            except Exception as e:
                logger.error(f"Error writing tick data to QuestDB: {e}")
            
            # Push completed timeframe data to QuestDB
            for timeframe_data in completed_timeframes:
                try:
                    qdb_writer.insert_candlestick(timeframe_data)
                    logger.info(f"Pushed {timeframe_data['timeframe']} data for {symbol}")
                except Exception as e:
                    logger.error(f"Error writing {timeframe_data['timeframe']} data to QuestDB: {e}")
                
        # Set the handler in IBKR client
        ibkr_client.set_market_data_handler(handle_market_data)
        
        # Subscribe to real-time market data for all contracts
        ibkr_client.subscribe_to_market_data(all_contracts_to_process)
        logger.info("All subscriptions active. Collecting real-time data...")
        
        # Main loop to keep the program running and print statistics
        no_data_counter = 0
        last_process_time = time.time()
        try:
            while True:
                # Print statistics every 5 seconds
                current_time = time.time()
                if current_time - last_stats_time >= 5.0:
                    elapsed = current_time - start_time
                    rate = tick_count / elapsed if elapsed > 0 else 0
                    logger.info(f"Statistics: {tick_count} ticks processed ({rate:.1f}/sec)")
                    
                    # Print top symbols by volume
                    sorted_symbols = sorted(tick_counter_per_symbol.items(), 
                                           key=lambda x: x[1], reverse=True)
                    for symbol, count in sorted_symbols[:5]:
                        logger.info(f"  {symbol}: {count} ticks")
                    
                    # Flush QuestDB data periodically if connected
                    if qdb_writer:
                        try:
                            rows_flushed = qdb_writer.flush()
                            if rows_flushed > 0:
                                logger.info(f"Data flushed to QuestDB")
                        except Exception as e:
                            logger.error(f"Error flushing data to QuestDB: {e}")
                    
                    last_stats_time = current_time
                
                # Process any pending ticks every 0.5 seconds
                if current_time - last_process_time >= 0.5:
                    ibkr_client.process_pending_ticks()
                    last_process_time = current_time
                
                # Check for no data condition (simulate data if needed)
                if current_time - last_data_time >= 10.0:
                    no_data_seconds = int(current_time - last_data_time)
                    if no_data_seconds % 10 == 0:
                        logger.info(f"No new market data for {no_data_seconds:.1f} seconds")
                        
                        # Enable simulation after a while
                        if no_data_seconds >= 20:
                            if not ibkr_client.data_simulation_enabled:
                                logger.info("Enabling data simulation mode due to lack of market data...")
                                ibkr_client.data_simulation_enabled = True
                    
                    # Force immediate simulation attempt if it's enabled
                    if ibkr_client.data_simulation_enabled:
                        ibkr_client._simulate_market_data()
                    
                # Sleep to reduce CPU usage
                time.sleep(0.1)  # Shorter sleep for more responsive processing
                
        except KeyboardInterrupt:
            logger.info("Stopping data collection...")
        
        # Final flush before exit
        if qdb_writer:
            logger.info("Flushing final data to QuestDB...")
            try:
                qdb_writer.flush()
            except Exception as e:
                logger.error(f"Error flushing final data to QuestDB: {e}")
        
    finally:
        # Clean up and disconnect
        logger.info("Disconnecting from IBKR...")
        ibkr_client.disconnect()
        logger.info("Data collection finished")

if __name__ == "__main__":
    main()