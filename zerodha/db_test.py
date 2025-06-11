import asyncio
import logging
import time
import random
from datetime import datetime
from qdb import QuestDBClient
from questdb.ingress import TimestampNanos

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def insert_test_data():
    """Test directly inserting data into QuestDB"""
    # Create QuestDB client
    client = QuestDBClient()
    
    # Connect to QuestDB
    logger.info("Connecting to QuestDB...")
    connected = await client.connect()
    
    if not connected:
        logger.error("Failed to connect to QuestDB")
        return False
    
    logger.info("Connected to QuestDB")
    
    # Generate test data
    test_data = []
    for i in range(10):
        timestamp = int(time.time() * 1_000_000_000)  # Current time in nanoseconds
        
        data = {
            "timestamp": timestamp,
            "instrument_token": 100000 + i,
            "exchange": "NSE",
            "trading_symbol": f"TEST{i}",
            "last_price": random.uniform(100, 1000),
            "last_quantity": random.randint(1, 100),
            "average_price": random.uniform(100, 1000),
            "volume": random.randint(1000, 10000),
            "buy_quantity": random.randint(500, 5000),
            "sell_quantity": random.randint(500, 5000),
            "ohlc": {
                "open": random.uniform(90, 110),
                "high": random.uniform(110, 120),
                "low": random.uniform(80, 90),
                "close": random.uniform(90, 110)
            }
        }
        test_data.append(data)
    
    # Insert test data
    try:
        logger.info(f"Inserting {len(test_data)} test records...")
        success_count = client.insert_full_ticks_batch(test_data)
        logger.info(f"Successfully inserted {success_count} records")
        
        # Insert one more batch
        logger.info("Inserting second batch...")
        
        # Generate more test data
        test_data_2 = []
        for i in range(5):
            timestamp = int(time.time() * 1_000_000_000)  # Current time in nanoseconds
            
            data = {
                "timestamp": timestamp,
                "instrument_token": 200000 + i,
                "exchange": "BSE",
                "trading_symbol": f"TESTBSE{i}",
                "last_price": random.uniform(100, 1000),
                "last_quantity": random.randint(1, 100),
                "average_price": random.uniform(100, 1000),
                "volume": random.randint(1000, 10000),
                "buy_quantity": random.randint(500, 5000),
                "sell_quantity": random.randint(500, 5000),
                "ohlc": {
                    "open": random.uniform(90, 110),
                    "high": random.uniform(110, 120),
                    "low": random.uniform(80, 90),
                    "close": random.uniform(90, 110)
                }
            }
            test_data_2.append(data)
        
        success_count_2 = client.insert_full_ticks_batch(test_data_2)
        logger.info(f"Successfully inserted {success_count_2} records in second batch")
        
        return True
    except Exception as e:
        logger.error(f"Error inserting test data: {e}")
        return False
    finally:
        # Close connection
        await client.close()
        logger.info("Closed QuestDB connection")

async def main():
    print("QuestDB Direct Insertion Test")
    print("============================\n")
    
    success = await insert_test_data()
    
    if success:
        print("\n QuestDB insertion test PASSED")
        print("\nSuccessfully inserted test data into QuestDB.")
    else:
        print("\n QuestDB insertion test FAILED - See errors above")

if __name__ == "__main__":
    asyncio.run(main()) 