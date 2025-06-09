import asyncio
import json
import logging
import time
import aiohttp
import os
import ssl
import urllib.parse
from datetime import datetime
from typing import Dict, Optional
from binance_streams import STREAM_TYPES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




class DataFlowMonitor:
    def __init__(self, metrics_file: str = "metrics_copy.json"):
        self.metrics_file = metrics_file
        self.host = os.getenv('QUESTDB_HOST', 'qdb3.satyvm.com')
        self.port = int(os.getenv('QUESTDB_PORT', '443'))
        self.username = os.getenv('QUESTDB_USER', '2Cents')
        self.password = os.getenv('QUESTDB_PASSWORD', '2Cents$1012cc')
        self.questdb_url = f"https://{self.host}:{self.port}/exec"
        self.stream_types = ['markPrice', 'kline', 'aggTrade', 'bookTicker', 'ticker']
        self.metrics = {
            stream_type: {
                'received_count': 0,
                'inserted_count': 0,
                'last_received': 0,
                'last_inserted': 0,
                'queue_size': 0,
                'data_loss_events': 0
            } for stream_type in self.stream_types
        }
        self.expected_frequencies = {
            'markPrice': 1.0,
            'ticker': 1.0,
            'aggTrade': 0.1,
            'bookTicker': 0.1,
            'kline': 60.0
        }
        self.batch_timeouts = {
            'markPrice': 0.5,
            'ticker': 1.0,
            'aggTrade': 0.1,
            'bookTicker': 0.2,
            'kline': 1.0
        }
        self.max_retries = 3
        self.retry_delay = 5
        self.timestamp_columns = {
            'agg_trades': 'transaction_time',
            'mark_prices': 'timestamp',
            'tickers': 'timestamp',
            'book_tickers': 'timestamp',
            'klines': 'timestamp'
        }

    async def _query_questdb_count(self, table_name: str, time_window: int = 30) -> int:
        """Query the count of records in QuestDB for the last time_window seconds with retry"""
        timestamp_column = self.timestamp_columns.get(table_name, 'timestamp')
        query = f"""
            SELECT count(*) 
            FROM {table_name}
            WHERE {timestamp_column} >= dateadd('s', -{time_window}, now());
        """
        encoded_query = urllib.parse.urlencode({'query': query.strip()})
        url = f"{self.questdb_url}?{encoded_query}"
        
        async with aiohttp.ClientSession() as session:
            for attempt in range(self.max_retries):
                try:
                    ssl_context = ssl.create_default_context()
                    async with session.get(
                        url,
                        auth=aiohttp.BasicAuth(self.username, self.password),
                        ssl=ssl_context
                    ) as resp:
                        if resp.status == 200:
                            result = await resp.json()
                            return result['dataset'][0][0] if result['dataset'] else 0
                        else:
                            error_text = await resp.text()
                            logger.error(f"QuestDB query error (attempt {attempt + 1}/{self.max_retries}): {error_text}")
                            if "Invalid column" in error_text:
                                logger.warning(f"Invalid column for {table_name}. Check QuestDB table schema.")
                except Exception as e:
                    logger.error(f"Error querying QuestDB (attempt {attempt + 1}/{self.max_retries}) for table {table_name}: {e}")
                
                if attempt < self.max_retries - 1:
                    logger.info(f"Retrying QuestDB query in {self.retry_delay} seconds...")
                    await asyncio.sleep(self.retry_delay)
        
        logger.error(f"Failed to query QuestDB after {self.max_retries} attempts for table {table_name}")
        return 0

    async def _read_metrics(self) -> Dict:
        """Read metrics from the copied JSON file"""
        try:
            if os.path.exists(self.metrics_file):
                with open(self.metrics_file, 'r') as f:
                    content = f.read().strip()
                    if not content:
                        logger.error(f"Metrics file {self.metrics_file} is empty. Please ensure valid copy from metrics.json.")
                        return {}
                    metrics = json.load(f)
                    logger.info(f"Read metrics from {self.metrics_file}:\n{json.dumps(metrics, indent=2)}")
                    return metrics
            logger.error(f"Metrics file {self.metrics_file} not found. Please copy metrics.json to {self.metrics_file}.")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing metrics file {self.metrics_file}: Invalid JSON - {e}")
            logger.error(f"Content of {self.metrics_file}: {open(self.metrics_file, 'r').read() if os.path.exists(self.metrics_file) else 'File is empty or inaccessible'}")
            return {}
        except Exception as e:
            logger.error(f"Error reading metrics file {self.metrics_file}: {e}")
            return {}

    async def check_data_loss(self):
        """Check for data loss using the copied metrics file"""
        try:
            metrics_data = await self._read_metrics()
            current_time = time.time()
            report = {"timestamp": datetime.utcnow().isoformat(), "streams": {}}
            
            for stream_type in self.stream_types:
                table_name = STREAM_TYPES[stream_type]['table']
                metrics = self.metrics[stream_type]
                stream_metrics = metrics_data.get(stream_type, {})
                
                received_count = stream_metrics.get('received_count', 0)
                queue_size = stream_metrics.get('queue_size', 0)
                last_update = stream_metrics.get('last_update', 0)
                
                metrics['received_count'] = received_count
                metrics['queue_size'] = queue_size
                metrics['last_received'] = last_update
                
                inserted_count = await self._query_questdb_count(table_name, time_window=300)
                metrics['inserted_count'] = inserted_count
                if inserted_count > 0:
                    metrics['last_inserted'] = current_time
                
                if received_count > inserted_count * 1.1 and received_count - inserted_count > 50:
                    metrics['data_loss_events'] += 1
                    logger.warning(
                        f"Potential data loss in {stream_type}: "
                        f"Received {received_count}, Inserted {inserted_count} "
                        f"in last 300 seconds. Check run_connector.py QuestDB connection."
                    )
                
                if queue_size > 800:
                    logger.warning(
                        f"High queue size for {stream_type}: {queue_size} "
                        f"(threshold: 800). Check run_connector.py QuestDB insertions."
                    )
                
                report['streams'][stream_type] = {
                    'received_count': metrics['received_count'],
                    'inserted_count': metrics['inserted_count'],
                    'queue_size': metrics['queue_size'],
                    'data_loss_events': metrics['data_loss_events'],
                    'last_received': (
                        datetime.fromtimestamp(metrics['last_received']).isoformat()
                        if metrics['last_received'] else None
                    ),
                    'last_inserted': (
                        datetime.fromtimestamp(metrics['last_inserted']).isoformat()
                        if metrics['last_inserted'] else None
                    ),
                    'status': (
                        'healthy' if metrics['last_received'] and 
                        current_time - metrics['last_received'] < self.expected_frequencies[stream_type] * 3
                        else 'stale'
                    ),
                    'qdb_count': inserted_count
                }
            
            logger.info(f"Data Loss Report:\n{json.dumps(report, indent=2)}")
        except Exception as e:
            logger.error(f"Error checking data loss: {e}")

    async def run(self):
        """Run the data loss check once"""
        try:
            logger.info(f"Starting data loss check using {self.metrics_file}")
            await self.check_data_loss()
        except Exception as e:
            logger.error(f"Error in monitor: {e}")
        finally:
            logger.info("Completed data loss check")

async def main():
    monitor = DataFlowMonitor()
    await monitor.run()

if __name__ == "__main__":
    asyncio.run(main())