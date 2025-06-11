import os
import logging
import requests
import time
from typing import Dict, Any
from pathlib import Path
from dotenv import load_dotenv
from config import TABLE_SCHEMAS, QDB_HOST, QDB_PORT, QDB_USER, QDB_PASSWORD

# Load environment variables from parent directory
ENV_PATH = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(ENV_PATH)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_tables(questdb_url: str = None) -> Dict[str, bool]: # type:ignore
    """Create tables in QuestDB if they don't exist."""
    
    if not questdb_url:
        questdb_url = f"https://{QDB_HOST}:{QDB_PORT}"
    
    # Authentication for QuestDB
    auth = None
    if QDB_USER and QDB_PASSWORD:
        auth = (QDB_USER, QDB_PASSWORD)
        logger.info(f"Using authentication with user: {QDB_USER}")
    
    results = {}
    
    logger.info("Creating/verifying QuestDB tables...")
    
    # Create each table using the schemas from config
    for table_name, schema in TABLE_SCHEMAS.items():
        try:
            # Use the /exec endpoint with GET method for table creation
            response = requests.get(
                f"{questdb_url}/exec",
                params={'query': schema},
                headers={'Accept': 'application/json'},
                auth=auth,
                timeout=30,
                verify=True
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"✓ Successfully created/verified table: {table_name}")
                results[table_name] = True
            else:
                logger.error(f"✗ Failed to create table {table_name}: {response.status_code} - {response.text}")
                results[table_name] = False
                
        except Exception as e:
            logger.error(f"✗ Error creating table {table_name}: {e}")
            results[table_name] = False
    
    # Verify tables were created by listing them
    try:
        response = requests.get(
            f"{questdb_url}/exec",
            params={"query": "SHOW TABLES;"},
            headers={'Accept': 'application/json'},
            auth=auth,
            timeout=30,
            verify=True
        )
        
        if response.status_code == 200:
            data = response.json()
            table_list = [row[0] for row in data.get('dataset', [])]
            
            # Check which tables were successfully created
            created_tables = [table for table in TABLE_SCHEMAS.keys() if table in table_list]
            missing_tables = [table for table in TABLE_SCHEMAS.keys() if table not in table_list]
            
            if created_tables:
                logger.info(f"✓ Verified existing tables: {', '.join(created_tables)}")
            
            if missing_tables:
                logger.warning(f"⚠ Missing tables: {', '.join(missing_tables)}")
            
        else:
            logger.error(f"Error listing tables: {response.status_code} - {response.text}")
            
    except Exception as e:
        logger.error(f"Error verifying tables: {e}")
    
    # Summary
    successful_tables = sum(1 for success in results.values() if success)
    total_tables = len(results)
    
    logger.info(f"Table creation summary: {successful_tables}/{total_tables} tables ready")
    
    if successful_tables == total_tables:
        logger.info("✓ All tables created successfully!")
    else:
        logger.warning(f"⚠ {total_tables - successful_tables} tables failed to create")
    
    return results

def test_connection():
    """Test connection to QuestDB"""
    try:
        questdb_url = f"https://{QDB_HOST}:{QDB_PORT}"
        auth = (QDB_USER, QDB_PASSWORD) if QDB_USER and QDB_PASSWORD else None
        
        response = requests.get(
            f"{questdb_url}/exec",
            params={"query": "SELECT 1;"},
            headers={'Accept': 'application/json'},
            auth=auth,
            timeout=10,
            verify=True
        )
        
        if response.status_code == 200:
            logger.info("✓ QuestDB connection test successful")
            return True
        else:
            logger.error(f"✗ QuestDB connection test failed: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"✗ QuestDB connection test failed: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    logger.info("Testing QuestDB connection...")
    if test_connection():
        logger.info("Creating tables...")
        results = create_tables()
        
        # Print final status
        if all(results.values()):
            logger.info("🎉 Setup complete! All tables are ready.")
        else:
            logger.error("❌ Setup incomplete. Check errors above.")
    else:
        logger.error("❌ Cannot connect to QuestDB. Check your configuration.")