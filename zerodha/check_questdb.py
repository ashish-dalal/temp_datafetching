import os
import sys
import logging
from dotenv import load_dotenv
from questdb.ingress import Sender, TimestampNanos
import time
import psycopg

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_questdb_connection():
    """Test connection to QuestDB and create tables if they don't exist"""
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
    except Exception as e:
        print(f" ILP connection failed: {e}")
        return False
    
    # Test PostgreSQL connection (for table creation and validation)
    try:
        print("\nTesting PostgreSQL connection (for table creation)...")
        # Connect to QuestDB via PostgreSQL interface
        conn_string = f"postgresql://{user}:{password}@{host}:{port}/qdb"
        print(f"Connection string: postgresql://{user}:***@{host}:{port}/qdb")
        
        with psycopg.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # Check for existing tables
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                existing_tables = [row[0] for row in cur.fetchall()]
                
                print(f"\nExisting tables: {', '.join(existing_tables) if existing_tables else 'None'}")
                
                # Check if our required tables exist
                required_tables = ['ticks_full', 'ticks_quote', 'ticks_ltp']
                missing_tables = [table for table in required_tables if table not in existing_tables]
                
                if missing_tables:
                    print(f"\nMissing tables: {', '.join(missing_tables)}")
                    
                    # Create missing tables
                    for table in missing_tables:
                        print(f"\nCreating table: {table}")
                        if table == 'ticks_full':
                            cur.execute("""
                            CREATE TABLE ticks_full (
                                timestamp TIMESTAMP,
                                exchange SYMBOL,
                                trading_symbol SYMBOL,
                                instrument_token LONG,
                                last_price DOUBLE,
                                last_quantity LONG,
                                average_price DOUBLE,
                                volume LONG,
                                buy_quantity LONG,
                                sell_quantity LONG,
                                open DOUBLE,
                                high DOUBLE,
                                low DOUBLE,
                                close DOUBLE
                            ) timestamp(timestamp) PARTITION BY DAY;
                            """)
                        elif table == 'ticks_quote':
                            cur.execute("""
                            CREATE TABLE ticks_quote (
                                timestamp TIMESTAMP,
                                exchange SYMBOL,
                                trading_symbol SYMBOL,
                                instrument_token LONG,
                                last_price DOUBLE,
                                last_quantity LONG,
                                average_price DOUBLE,
                                volume LONG,
                                buy_quantity LONG,
                                sell_quantity LONG,
                                open DOUBLE,
                                high DOUBLE,
                                low DOUBLE,
                                close DOUBLE,
                                last_trade_time TIMESTAMP
                            ) timestamp(timestamp) PARTITION BY DAY;
                            """)
                        elif table == 'ticks_ltp':
                            cur.execute("""
                            CREATE TABLE ticks_ltp (
                                timestamp TIMESTAMP,
                                exchange SYMBOL,
                                trading_symbol SYMBOL,
                                instrument_token LONG,
                                last_price DOUBLE
                            ) timestamp(timestamp) PARTITION BY DAY;
                            """)
                        print(f" Created table: {table}")
                    
                    conn.commit()
                    print("\n All required tables created successfully!")
                else:
                    print("\n All required tables already exist!")
                
                # Verify table structure
                for table in required_tables:
                    print(f"\nVerifying structure of table: {table}")
                    cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
                    columns = cur.fetchall()
                    print(f"Table {table} columns:")
                    for col_name, col_type in columns:
                        print(f"  - {col_name}: {col_type}")
                
                print("\n PostgreSQL connection successful! Tables verified.")
                return True
    except Exception as e:
        print(f" PostgreSQL connection failed: {e}")
        return False

if __name__ == "__main__":
    print("QuestDB Connection Test")
    print("======================\n")
    
    success = test_questdb_connection()
    
    if success:
        print("\n QuestDB connectivity test PASSED")
        sys.exit(0)
    else:
        print("\n QuestDB connectivity test FAILED - See errors above")
        sys.exit(1) 