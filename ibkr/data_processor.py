from config import TABLES
from datetime import datetime, date
import time
import logging

logger = logging.getLogger(__name__)




def process_data(raw_data):
    if not raw_data:
        return None
    
    try:
        asset_class = raw_data.get('asset_class', 'Unknown')
        symbol = raw_data['symbol']
        exchange = raw_data['exchange']
        country = raw_data['country']
        timestamp = raw_data['timestamp']
        ilp_line = None

        # Determine if this is tick data (price, volume, bid, ask)
        is_tick_data = any(field in raw_data for field in ['price', 'volume', 'bid', 'ask'])
        
        if is_tick_data:
            # Tick data
            table = TABLES['ticks'].get(asset_class, 'unknown_ticks')
            tags = f"symbol={symbol},exchange={exchange},country={country}"
            fields = []
            
            # Add all available price fields
            if 'price' in raw_data:
                fields.append(f"price={raw_data['price']}")
            if 'volume' in raw_data:
                fields.append(f"volume={raw_data['volume']}")
            if 'bid' in raw_data:
                fields.append(f"bid={raw_data['bid']}")
            if 'ask' in raw_data:
                fields.append(f"ask={raw_data['ask']}")
                
            if not fields:
                logger.warning(f"No valid fields found in tick data: {raw_data}")
                return None
                
            fields_str = ",".join(fields)
            
            try:
                if isinstance(timestamp, (str, int, float)):
                    ts = int(float(timestamp) * 1_000_000_000)
                else:
                    # Default handling for unknown types
                    ts = int(time.time() * 1_000_000_000)
            except Exception as e:
                logger.error(f"Timestamp conversion error in tick data: {e}, type: {type(timestamp)}, value: {timestamp}")
                ts = int(time.time() * 1_000_000_000)
            
            ilp_line = f"{table} {tags} {fields_str} {ts}"
            
        else:
            # Candlestick data
            table = TABLES['candlesticks'].get(asset_class, 'unknown_candlesticks')
            timeframe = raw_data.get('timeframe', 'Unknown').replace(' ', '')
            tags = f"symbol={symbol},exchange={exchange},country={country},timeframe={timeframe}"
            fields = (
                f"open={raw_data['open']},high={raw_data['high']},"
                f"low={raw_data['low']},close={raw_data['close']},volume={raw_data['volume']}"
            )
            
            try:
                # Handle different timestamp formats
                if isinstance(timestamp, str):
                    ts = int(datetime.strptime(timestamp, '%Y%m%d  %H:%M:%S').timestamp() * 1_000_000_000)
                elif isinstance(timestamp, datetime):
                    ts = int(timestamp.timestamp() * 1_000_000_000)
                elif isinstance(timestamp, date):
                    # Convert date to datetime at midnight
                    dt = datetime.combine(timestamp, datetime.min.time())
                    ts = int(dt.timestamp() * 1_000_000_000)
                else:
                    ts = int(time.time() * 1_000_000_000)
            except (ValueError, TypeError, AttributeError) as e:
                # Fallback handling
                logger.error(f"Timestamp conversion error: {e}, type: {type(timestamp)}, value: {timestamp}")
                ts = int(time.time() * 1_000_000_000)
                
            ilp_line = f"{table} {tags} {fields} {ts}"

        return ilp_line
    except Exception as e:
        logger.error(f"Error processing data: {e}, data: {raw_data}")
        return None