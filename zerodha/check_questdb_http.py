import os
import sys
import logging
import requests
import json
from dotenv import load_dotenv
from questdb.ingress import Sender, TimestampNanos
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_questdb_connection():
    """Test connection to QuestDB using ILP protocol"""
    # Load environment variables
    load_dotenv()
    
    # Get connection details
    host = os.getenv('QUESTDB_HOST')
    port = os.getenv('QUESTDB_PORT')
    user = os.getenv('QUESTDB_USER')
    password = os.getenv('QUESTDB_PASSWORD')
    
    # Print connection details (with password masked)
    print(f"QuestDB Connection Info:")
    print(f"Host: {host}")
    print(f"Port: {port}")
    print(f"User: {user}")
    print(f"Password: {'*' * len(password) if password else 'Not set'}")
    
    # Verify we have all required connection parameters
    missing = []
    if not host:
        missing.append("QUESTDB_HOST")
    if not port:
        missing.append("QUESTDB_PORT")
    if not user:
        missing.append("QUESTDB_USER")
    if not password:
        missing.append("QUESTDB_PASSWORD")
        
    if missing:
        error_msg = f"Missing QuestDB configuration parameters: {', '.join(missing)}"
        logger.error(error_msg)
        return False
    
    # Test ILP connection (used for data insertion)
    try:
        print("\nTesting ILP connection (for data insertion)...")
        conf = f"https::addr={host}:{port};username={user};password={password};"
        print(f"Connection string: https::addr={host}:{port};username={user};password=***")
        
        # Try to create a sender
        with Sender.from_conf(conf) as sender:
            # Generate a test timestamp
            ts = TimestampNanos(int(time.time() * 1_000_000_000))
            
            # Insert test data
            sender.row(
                'test_connection',
                symbols={
                    'source': 'test_script'
                },
                columns={
                    'value': 1.0,
                    'status': 'OK'
                },
                at=ts
            )
            sender.flush()
            print(" ILP connection successful! Test data inserted.")
            
            # Try another test record to confirm data is being written
            ts = TimestampNanos(int(time.time() * 1_000_000_000))
            sender.row(
                'test_connection',
                symbols={
                    'source': 'test_script_2'
                },
                columns={
                    'value': 2.0,
                    'status': 'OK'
                },
                at=ts
            )
            sender.flush()
            print(" ILP connection verified with second test record.")
            
            # Now try to create the required tables for our application
            print("\nCreating required tables via ILP protocol...")
            
            # Create ticks_full table
            ts = TimestampNanos(int(time.time() * 1_000_000_000))
            sender.row(
                'ticks_full',
                symbols={
                    'exchange': 'TEST',
                    'trading_symbol': 'TEST'
                },
                columns={
                    'instrument_token': 123456,
                    'last_price': 100.0,
                    'last_quantity': 10,
                    'average_price': 100.0,
                    'volume': 1000,
                    'buy_quantity': 500,
                    'sell_quantity': 500,
                    'open': 99.0,
                    'high': 101.0,
                    'low': 98.0,
                    'close': 100.0
                },
                at=ts
            )
            
            # Create ticks_quote table
            sender.row(
                'ticks_quote',
                symbols={
                    'exchange': 'TEST',
                    'trading_symbol': 'TEST'
                },
                columns={
                    'instrument_token': 123456,
                    'last_price': 100.0,
                    'last_quantity': 10,
                    'average_price': 100.0,
                    'volume': 1000,
                    'buy_quantity': 500,
                    'sell_quantity': 500,
                    'open': 99.0,
                    'high': 101.0,
                    'low': 98.0,
                    'close': 100.0,
                    'last_trade_time': int(time.time() * 1_000_000_000)
                },
                at=ts
            )
            
            # Create ticks_ltp table
            sender.row(
                'ticks_ltp',
                symbols={
                    'exchange': 'TEST',
                    'trading_symbol': 'TEST'
                },
                columns={
                    'instrument_token': 123456,
                    'last_price': 100.0
                },
                at=ts
            )
            
            sender.flush()
            print(" Sample data inserted for all required tables.")
            
            return True
            
    except Exception as e:
        print(f"❌ ILP connection failed: {e}")
        return False

if __name__ == "__main__":
    print("QuestDB Connection Test (ILP Only)")
    print("================================\n")
    
    success = test_questdb_connection()
    
    if success:
        print("\n QuestDB ILP connectivity test PASSED")
        print("\nTables should now exist in QuestDB with the proper schema.")
        print("You may need to wait a few seconds for QuestDB to create the tables.")
        sys.exit(0)
    else:
        print("\n QuestDB connectivity test FAILED - See errors above")
        sys.exit(1) 