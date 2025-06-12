#!/usr/bin/env python3

"""
IBKR Connection Test Script
Tests connectivity to both IBKR Gateway and QuestDB
"""

import asyncio
import logging
import json
import sys
from pathlib import Path
from dotenv import load_dotenv
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(ENV_PATH)

async def test_ibkr_connection():
    """Test IBKR Gateway connection"""
    logger.info("🔗 Testing IBKR Gateway connection...")
    
    try:
        from ib_insync import IB
        
        ib = IB()
        host = os.getenv('IBKR_HOST', '127.0.0.1')
        port = int(os.getenv('IBKR_PORT', 7497))
        client_id = int(os.getenv('IBKR_CLIENT_ID', 0))
        
        logger.info(f"Connecting to {host}:{port} with client_id {client_id}...")
        
        await ib.connectAsync(host=host, port=port, clientId=client_id, timeout=10)
        
        if ib.isConnected():
            logger.info("✅ IBKR Gateway connection successful!")
            
            # Test a simple contract qualification
            from ib_insync import Stock
            contract = Stock('AAPL', 'SMART', 'USD')
            qualified = await ib.qualifyContractsAsync(contract)
            
            if qualified:
                logger.info(f"✅ Contract qualification successful: {qualified[0].symbol} ({qualified[0].conId})")
                ib.disconnect()
                return 0  # All good
            else:
                logger.warning("⚠️ Contract qualification failed")
                ib.disconnect()
                return 61  # Contract qualification failed
            
        else:
            logger.error("❌ IBKR Gateway connection failed")
            return 60  # Connection failed
            
    except Exception as e:
        logger.error(f"❌ IBKR Gateway connection error: {e}")
        return 60  # Connection failed

def test_questdb_connection():
    """Test QuestDB connection"""
    logger.info("🔗 Testing QuestDB connection...")
    
    connection_failed = False
    missing_tables = []
    push_failures = []
    
    try:
        import requests
        
        host = os.getenv('QUESTDB_HOST', 'localhost')
        port = os.getenv('QUESTDB_PORT', '9000')
        user = os.getenv('QUESTDB_USER')
        password = os.getenv('QUESTDB_PASSWORD')
        
        url = f"https://{host}:{port}/exec"
        auth = (user, password) if user and password else None
        
        logger.info(f"Connecting to QuestDB at {host}:{port}...")
        
        response = requests.get(
            url,
            params={"query": "SELECT 1 as test;"},
            auth=auth,
            timeout=10,
            verify=True
        )
        
        if response.status_code == 200:
            logger.info("✅ QuestDB connection successful!")
            
            # Test table listing
            response = requests.get(
                url,
                params={"query": "SHOW TABLES;"},
                auth=auth,
                timeout=10,
                verify=True
            )
            
            if response.status_code == 200:
                data = response.json()
                tables = [row[0] for row in data.get('dataset', [])]
                logger.info(f"✅ Found {len(tables)} tables in QuestDB")
                
                # Check for IBKR tables
                required_tables = ['stocks_ticks', 'indices_ticks', 'stocks_candlesticks', 'indices_candlesticks']
                table_mapping = {
                    'stocks_ticks': 1,
                    'indices_ticks': 2,
                    'stocks_candlesticks': 3,
                    'indices_candlesticks': 4
                }
                
                ibkr_tables = [t for t in tables if t in required_tables]
                if ibkr_tables:
                    logger.info(f"✅ IBKR tables found: {', '.join(ibkr_tables)}")
                    
                    # Check for missing tables
                    for table in required_tables:
                        if table not in tables:
                            missing_tables.append(table_mapping[table])
                    
                    if missing_tables:
                        logger.warning(f"⚠️ Missing tables: {[t for t in required_tables if t not in tables]}")
                    
                    # Test data push to verify write functionality
                    logger.info("🧪 Testing data push to QuestDB...")
                    test_queries = []
                    
                    if 'stocks_ticks' in ibkr_tables:
                        test_queries.append((
                            "INSERT INTO stocks_ticks (symbol, price, bid, ask, volume, timestamp) "
                            "VALUES ('TEST_DATA', 0, 0, 0, 0, now());",
                            1
                        ))
                    
                    if 'indices_ticks' in ibkr_tables:
                        test_queries.append((
                            "INSERT INTO indices_ticks (symbol, price, bid, ask, volume, timestamp) "
                            "VALUES ('TEST_DATA', 0, 0, 0, 0, now());",
                            2
                        ))
                    
                    if 'stocks_candlesticks' in ibkr_tables:
                        test_queries.append((
                            "INSERT INTO stocks_candlesticks (symbol, timeframe, open, high, low, close, volume, timestamp) "
                            "VALUES ('TEST_DATA', 'TEST_DATA', 0, 0, 0, 0, 0, now());",
                            3
                        ))
                    
                    if 'indices_candlesticks' in ibkr_tables:
                        test_queries.append((
                            "INSERT INTO indices_candlesticks (symbol, timeframe, open, high, low, close, volume, timestamp) "
                            "VALUES ('TEST_DATA', 'TEST_DATA', 0, 0, 0, 0, 0, now());",
                            4
                        ))
                    
                    # Execute test inserts
                    successful_inserts = 0
                    for query, table_id in test_queries:
                        try:
                            test_response = requests.get(
                                url,
                                params={"query": query},
                                auth=auth,
                                timeout=10,
                                verify=True
                            )
                            if test_response.status_code == 200:
                                successful_inserts += 1
                            else:
                                logger.warning(f"⚠️ Test insert failed for table {table_id}: {test_response.status_code}")
                                push_failures.append(table_id)
                        except Exception as e:
                            logger.warning(f"⚠️ Test insert error for table {table_id}: {e}")
                            push_failures.append(table_id)
                    
                    if successful_inserts > 0:
                        logger.info(f"✅ Successfully pushed test data to {successful_inserts} tables")
                    
                    if len(push_failures) == len(test_queries) and test_queries:
                        logger.warning("⚠️ Failed to push test data to any tables")
                        
                else:
                    logger.warning("⚠️ No IBKR tables found")
                    missing_tables = [1, 2, 3, 4]  # All tables missing
            
        else:
            logger.error(f"❌ QuestDB connection failed: {response.status_code}")
            connection_failed = True
            
    except Exception as e:
        logger.error(f"❌ QuestDB connection error: {e}")
        connection_failed = True
    
    # Calculate return code
    if connection_failed:
        return 10
    
    if missing_tables:
        if len(missing_tables) == 4:
            return 20  # No tables exist
        else:
            # Build code like 21, 212, 2134 etc.
            code_str = "2" + "".join(map(str, sorted(missing_tables)))
            return int(code_str)
    
    if push_failures:
        if len(push_failures) == len([t for t in ['stocks_ticks', 'indices_ticks', 'stocks_candlesticks', 'indices_candlesticks'] if t in ibkr_tables]):
            return 30  # Failed to push to any table
        else:
            # Build code like 31, 312, 3134 etc.
            code_str = "3" + "".join(map(str, sorted(push_failures)))
            return int(code_str)
    
    return 0  # All tests passed

def test_configuration():
    """Test configuration and environment setup"""
    logger.info("🔧 Testing configuration...")
    
    required_vars = [
        'IBKR_HOST', 'IBKR_PORT', 'IBKR_CLIENT_ID',
        'QUESTDB_HOST', 'QUESTDB_PORT', 'QUESTDB_USER', 'QUESTDB_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        return 50  # Missing environment variables
    
    logger.info("✅ All required environment variables are set")
    
    # Check IBKR_TICKERS.json
    tickers_file = Path(__file__).resolve().parent / 'IBKR_TICKERS.json'
    if not tickers_file.exists():
        logger.error("❌ IBKR_TICKERS.json file not found")
        return 51  # IBKR_TICKERS.json not found
    
    try:
        with open(tickers_file, 'r') as f:
            tickers = json.load(f)
        
        if not tickers:
            logger.warning("⚠️ IBKR_TICKERS.json is empty")
            return 52  # IBKR_TICKERS.json empty/invalid
        else:
            logger.info(f"✅ Loaded {len(tickers)} symbols from IBKR_TICKERS.json")
            
            # Show first few symbols - handle dictionary structure
            ticker_values = list(tickers.values())
            sample = ticker_values[:3] if len(ticker_values) > 3 else ticker_values
            for ticker in sample:
                symbol = ticker.get('SYMBOL', 'N/A')
                sec_type = ticker.get('secType', 'N/A')
                logger.info(f"   📊 {symbol} ({sec_type})")
        
        return 0  # All good
        
    except Exception as e:
        logger.error(f"❌ Error reading IBKR_TICKERS.json: {e}")
        return 52  # IBKR_TICKERS.json empty/invalid

async def main():
    """Run all connection tests"""
    logger.info("🚀 Starting IBKR Connection Tests")
    logger.info("=" * 50)
    
    # Test configuration
    config_result = test_configuration()
    print()
    
    # Test QuestDB
    questdb_result = test_questdb_connection()
    print()
    
    # Test IBKR Gateway
    ibkr_result = await test_ibkr_connection()
    print()
    
    # Summary
    logger.info("📋 Test Summary")
    logger.info("=" * 50)
    logger.info(f"Configuration: {'✅ PASS' if config_result == 0 else f'❌ FAIL (code: {config_result})'}")
    logger.info(f"QuestDB:       {'✅ PASS' if questdb_result == 0 else f'❌ FAIL (code: {questdb_result})'}")
    logger.info(f"IBKR Gateway:  {'✅ PASS' if ibkr_result == 0 else f'❌ FAIL (code: {ibkr_result})'}")
    
    if all([config_result == 0, questdb_result == 0, ibkr_result == 0]):
        logger.info("\n🎉 All tests passed! System is ready to run.")
        logger.info("Start the connector with: python run_connector.py")
        return 0
    else:
        logger.error("\n❌ Some tests failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("\n⏹️ Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n💥 Unexpected error: {e}")
        sys.exit(1)