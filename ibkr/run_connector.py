import asyncio
import json
import logging
import time
import os
from pathlib import Path
from dotenv import load_dotenv
from qdb import QuestDBClient
from ibkr_connector import IBKRConnector

# Load environment variables from parent directory
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(ENV_PATH)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics file to share with monitor.py
METRICS_FILE = "metrics.json"

def update_metrics(data_type: str, received: int, queue_size: int):
    """Update metrics in a JSON file"""
    try:
        metrics = {}
        if os.path.exists(METRICS_FILE):
            with open(METRICS_FILE, 'r') as f:
                metrics = json.load(f)
        
        if data_type not in metrics:
            metrics[data_type] = {'received_count': 0, 'queue_size': 0, 'last_update': 0}
        
        metrics[data_type]['received_count'] = received
        metrics[data_type]['queue_size'] = queue_size
        metrics[data_type]['last_update'] = time.time()
        
        with open(METRICS_FILE, 'w') as f:
            json.dump(metrics, f, indent=2)
    except Exception as e:
        logger.error(f"Error updating metrics: {e}")

async def main():
    """Main entry point for IBKR data connector"""
    logger.info("Starting IBKR Real-time Data Connector...")
    
    # Create async queue for data processing
    queue = asyncio.Queue(maxsize=10000)
    qdb_client = QuestDBClient()
    
    # Load symbols from IBKR_TICKERS.json
    tickers_file = Path(__file__).resolve().parent / 'IBKR_TICKERS_TEST.json'
    try:
        with open(tickers_file, 'r') as f:
            symbols_dict = json.load(f)
        logger.info(f"Loaded {len(symbols_dict)} symbols from IBKR_TICKERS.json")
    except Exception as e:
        logger.error(f"Error loading symbols: {e}")
        return
    
    # Get IBKR connection parameters from environment
    ibkr_host = os.getenv('IBKR_HOST', '127.0.0.1')
    ibkr_port = int(os.getenv('IBKR_PORT', 7497))
    client_id = int(os.getenv('IBKR_CLIENT_ID', 0))
    
    # Initialize IBKR connector
    connector = IBKRConnector(
        queue=queue,
        symbols_dict=symbols_dict,
        host=ibkr_host,
        port=ibkr_port,
        client_id=client_id
    )
    
    # Try connecting to QuestDB with retry logic
    max_retries = 5
    retry_count = 0
    db_connected = False
    
    while retry_count < max_retries and not db_connected:
        try:
            await qdb_client.connect()
            logger.info("Successfully connected to QuestDB")
            db_connected = True
        except Exception as e:
            retry_count += 1
            wait_time = 2 ** retry_count  # Exponential backoff
            logger.error(f"QuestDB connection failed (attempt {retry_count}/{max_retries}): {e}")
            
            if retry_count < max_retries:
                logger.info(f"Retrying connection in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger.warning("Max retries reached. Running without database storage.")
    
    # Database writer task
    async def db_writer():
        """Process queue and write data to QuestDB in batches"""
        batch_size = 100
        batch_timeout = 2.0
        batch = []
        last_flush = time.time()
        
        while True:
            try:
                # Get data from queue with timeout
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=0.1)
                    batch.append(data)
                    queue.task_done()
                except asyncio.TimeoutError:
                    pass
                
                # Flush batch if conditions are met
                current_time = time.time()
                should_flush = (
                    len(batch) >= batch_size or
                    (len(batch) > 0 and current_time - last_flush >= batch_timeout)
                )
                
                if should_flush and db_connected:
                    try:
                        if batch:
                            await qdb_client.insert_batch(batch)
                            logger.debug(f"Flushed {len(batch)} records to QuestDB")
                            batch = []
                            last_flush = current_time
                    except Exception as e:
                        logger.error(f"Error writing to QuestDB: {e}")
                        # Don't lose the batch, try again next time
                        
            except Exception as e:
                logger.error(f"Error in db_writer: {e}")
                await asyncio.sleep(1)
    
    # Start database writer task
    db_task = None
    if db_connected:
        db_task = asyncio.create_task(db_writer())
        logger.info("Database writer task started")
    
    # Start IBKR connector
    connector_task = asyncio.create_task(connector.start())
    logger.info("IBKR connector task started")
    
    # Run until interrupted
    try:
        if db_task:
            await asyncio.gather(connector_task, db_task)
        else:
            await connector_task
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Cleanup
        await connector.stop()
        if db_task:
            db_task.cancel()
        logger.info("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())