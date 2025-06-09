import os
import logging
import requests
import time
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from pathlib import Path

ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(ENV_PATH)
QDB_HOST = os.getenv('QUESTDB_HOST')
QDB_PORT = os.getenv('QUESTDB_PORT')
QDB_USER = os.getenv('QUESTDB_USER')
QDB_PASSWORD = os.getenv('QUESTDB_PASSWORD')

logger = logging.getLogger(__name__)

def create_tables(questdb_url: str = None) -> Dict[str, bool]:
    """Create tables in QuestDB if they don't exist."""
    
    if not questdb_url:
        questdb_url = os.environ.get('QUESTDB_URL', f'https://{QDB_HOST}:{QDB_PORT}')
    
    logger.info(f"Creating tables in QuestDB at {questdb_url}")
    
    # Authentication credentials
    auth = None
    if QDB_USER and QDB_PASSWORD:
        auth = (QDB_USER, QDB_PASSWORD)
        logger.info(f"Using authentication with user: {QDB_USER}")
    
    # Define table structures
    tables = {
        'stocks_ticks': """
            CREATE TABLE IF NOT EXISTS stocks_ticks (
                symbol SYMBOL,
                price DOUBLE,
                bid DOUBLE,
                ask DOUBLE,
                volume LONG,
                timestamp TIMESTAMP
            ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """,
        'indices_ticks': """
            CREATE TABLE IF NOT EXISTS indices_ticks (
                symbol SYMBOL,
                price DOUBLE,
                bid DOUBLE,
                ask DOUBLE,
                volume LONG,
                timestamp TIMESTAMP
            ) TIMESTAMP(timestamp) PARTITION BY DAY;
        """,
        'stocks_candlesticks': """
            CREATE TABLE IF NOT EXISTS stocks_candlesticks (
                symbol SYMBOL,
                timeframe SYMBOL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume LONG,
                timestamp TIMESTAMP
            ) TIMESTAMP(timestamp) PARTITION BY MONTH;
        """,
        'indices_candlesticks': """
            CREATE TABLE IF NOT EXISTS indices_candlesticks (
                symbol SYMBOL,
                timeframe SYMBOL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume LONG,
                timestamp TIMESTAMP
            ) TIMESTAMP(timestamp) PARTITION BY MONTH;
        """
    }
    
    results = {}
    
    # Create each table
    for table_name, schema in tables.items():
        try:
            # Use the /exec endpoint with POST method and proper content type
            response = requests.get(
                f"{questdb_url}/exec",
                params={'query':schema},
                # data=schema,  # Send as raw SQL
                headers={'Accept': 'application/json'},
                auth=auth,
                timeout=5
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully created/verified table: {table_name}")
                results[table_name] = True
            else:
                logger.error(f"Failed to create table {table_name}: {response.status_code} - {response.text}")
                results[table_name] = False
                
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            results[table_name] = False
    
    # List existing tables to verify
    try:
        response = requests.get(
            f"{questdb_url}/exec",
            params={"query": "SHOW TABLES;"},
            auth=auth,
            timeout=30,
            verify=True  
        )
        
        if response.status_code == 200:
            data = response.json()
            table_list = [row[0] for row in data.get('dataset', [])]
            logger.info(f"Existing tables in QuestDB: {', '.join(table_list)}")
        else:
            logger.error(f"Error listing tables: {response.status_code} - {response.text}")
            
    except Exception as e:
        logger.error(f"Error listing tables: {e}")
    
    return results

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    create_tables() 