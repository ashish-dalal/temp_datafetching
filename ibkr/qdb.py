import asyncio
import json
import logging
from datetime import datetime
from typing import List
from questdb.ingress import Sender, TimestampNanos
import os
from dotenv import load_dotenv
import aiohttp
from config import TABLES, QDB_HOST, QDB_PORT, QDB_USER, QDB_PASSWORD



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuestDBClient:
    def __init__(self):
        self.conf = (
            f"https::addr={QDB_HOST}:{QDB_PORT};"
            f"username={QDB_USER};"
            f"password={QDB_PASSWORD};"
        )
        self.sender = None
        self.connected = False
        self.url = f"https://{QDB_HOST}:9000/exec"

    async def connect(self):
        try:
            logger.debug(f"Attempting to connect to QuestDB with config: {self.conf}")
            self.sender = Sender.from_conf(self.conf)
            self.connected = True
            await self.create_tables()
            logger.info("Connected to QuestDB")
        except Exception as e:
            logger.error(f"Failed to connect to QuestDB: {e}")
            self.connected = False
            raise

    async def create_tables(self):
        try:
            async with aiohttp.ClientSession() as session:
                # Tick tables
                for table in TABLES['ticks'].values():
                    schema = f"""
                        CREATE TABLE IF NOT EXISTS {table} (
                            symbol SYMBOL,
                            exchange SYMBOL,
                            country SYMBOL,
                            price DOUBLE,
                            volume LONG,
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY DAY;
                    """
                    async with session.post(
                        self.url,
                        json={"query": schema},
                        auth=aiohttp.BasicAuth(QDB_USER, QDB_PASSWORD)
                    ) as resp:
                        if resp.status == 200:
                            logger.info(f"Created/verified tick table: {table}")
                        else:
                            logger.error(f"Error creating tick table {table}: {await resp.text()}")

                # Candlestick tables
                for table in TABLES['candlesticks'].values():
                    schema = f"""
                        CREATE TABLE IF NOT EXISTS {table} (
                            symbol SYMBOL,
                            exchange SYMBOL,
                            country SYMBOL,
                            timeframe SYMBOL,
                            open DOUBLE,
                            high DOUBLE,
                            low DOUBLE,
                            close DOUBLE,
                            volume LONG,
                            timestamp TIMESTAMP
                        ) TIMESTAMP(timestamp) PARTITION BY MONTH;
                    """
                    async with session.post(
                        self.url,
                        json={"query": schema},
                        auth=aiohttp.BasicAuth(QDB_USER, QDB_PASSWORD)
                    ) as resp:
                        if resp.status == 200:
                            logger.info(f"Created Bird candlestick table: {table}")
                        else:
                            logger.error(f"Error creating candlestick table {table}: {await resp.text()}")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    def insert_batch(self, table: str, events: List[dict]):
        if not self.connected or self.sender is None:
            logger.warning("QuestDB connection lost, attempting to reconnect...")
            self.sender = Sender.from_conf(self.conf)
            self.connected = True
        try:
            with Sender.from_conf(self.conf) as sender:
                for data in events:
                    try:
                        # Skip zero volume data
                        if 'volume' in data and data['volume'] == 0:
                            logger.debug(f"Skipping zero volume data for {data['symbol']}")
                            continue
                        ts = TimestampNanos(int(float(data['timestamp']) * 1_000_000_000))
                        symbols = {
                            'symbol': data['symbol'],
                            'exchange': data['exchange'],
                            'country': data['country']
                        }
                        columns = {}
                        if 'price' in data:
                            columns['price'] = float(data['price'])
                        if 'volume' in data:
                            columns['volume'] = int(data['volume'])
                        if 'open' in data:
                            symbols['timeframe'] = data['timeframe'].replace(' ', '')
                            columns.update({
                                'open': float(data['open']),
                                'high': float(data['high']),
                                'low': float(data['low']),
                                'close': float(data['close']),
                                'volume': int(data['volume'])
                            })
                        sender.row(table, symbols=symbols, columns=columns, at=ts)
                    except Exception as e:
                        logger.error(f"Error inserting event: {e}, data: {data}")
                sender.flush()
            logger.debug(f"Inserted batch of {len(events)} events into {table}")
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            self.connected = False
            raise

    async def close(self):
        if self.sender and self.connected:
            try:
                self.sender.flush()
                self.sender.close()
                self.connected = False
                logger.info("Closed QuestDB connection")
            except Exception as e:
                logger.error(f"Error closing QuestDB connection: {e}")

async def db_writer(queue: asyncio.Queue, qdb_client: QuestDBClient, batch_size: int = 100, batch_timeout: float = 0.5):
    logger.info("DB writer started")
    batches = {table: [] for table in set(TABLES['ticks'].values()) | set(TABLES['candlesticks'].values())}
    max_retries = 5

    while True:
        try:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=batch_timeout)
                if 'asset_class' in event:
                    table = (TABLES['candlesticks' if 'open' in event else 'ticks'].get(event['asset_class'], 'unknown'))
                    batches[table].append(event)
            except asyncio.TimeoutError:
                pass

            while sum(len(batch) for batch in batches.values()) < batch_size:
                try:
                    event = queue.get_nowait()
                    if 'asset_class' in event:
                        table = (TABLES['candlesticks' if 'open' in event else 'ticks'].get(event['asset_class'], 'unknown'))
                        batches[table].append(event)
                except asyncio.QueueEmpty:
                    break

            for table, batch in batches.items():
                if batch:
                    try:
                        if not qdb_client.connected:
                            logger.warning("QuestDB connection lost, reconnecting...")
                            retry_count = 0
                            while retry_count < max_retries and not qdb_client.connected:
                                try:
                                    await qdb_client.connect()
                                    logger.info("Reconnected to QuestDB")
                                except Exception as e:
                                    retry_count += 1
                                    wait_time = 2 ** retry_count
                                    logger.error(f"Reconnection failed (attempt {retry_count}/{max_retries}): {e}")
                                    if retry_count < max_retries:
                                        await asyncio.sleep(wait_time)
                                    else:
                                        break
                        if qdb_client.connected:
                            qdb_client.insert_batch(table, batch)
                        else:
                            logger.warning(f"Skipping insertion of {len(batch)} events for {table}")
                    except Exception as e:
                        logger.error(f"Error inserting batch for {table}: {e}")
                    finally:
                        batch.clear()
        except Exception as e:
            logger.error(f"Error in db_writer: {e}")
            await asyncio.sleep(1)