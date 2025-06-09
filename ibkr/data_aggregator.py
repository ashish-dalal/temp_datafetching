from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any
import time

logger = logging.getLogger(__name__)




class DataAggregator:
    def __init__(self):
        # Define timeframes in seconds
        self.timeframes = {
            '1min': 60,
            '5min': 300,
            '15min': 900,
            '1hour': 3600,
            '4hour': 14400,
            '1day': 86400,
            '1week': 604800,
            '1month': 2592000  # Approximate
        }
        
        # Initialize data structures for each symbol and timeframe
        self.aggregated_data = {}  # symbol -> timeframe -> data
        self.last_update = {}  # symbol -> timeframe -> timestamp
        
    def _initialize_symbol_data(self, symbol: str):
        """Initialize data structures for a new symbol."""
        if symbol not in self.aggregated_data:
            self.aggregated_data[symbol] = {}
            self.last_update[symbol] = {}
            
            for timeframe in self.timeframes:
                self.aggregated_data[symbol][timeframe] = {
                    'open': None,
                    'high': None,
                    'low': None,
                    'close': None,
                    'volume': 0,
                    'count': 0
                }
                self.last_update[symbol][timeframe] = 0
    
    def _should_update_timeframe(self, symbol: str, timeframe: str, current_time: float) -> bool:
        """Check if it's time to update a specific timeframe."""
        if symbol not in self.last_update or timeframe not in self.last_update[symbol]:
            return True
            
        last_update = self.last_update[symbol][timeframe]
        timeframe_seconds = self.timeframes[timeframe]
        
        # Check if we've crossed the timeframe boundary
        return (current_time - last_update) >= timeframe_seconds
    
    def process_tick(self, tick_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a tick and return any completed timeframe data."""
        if not tick_data:
            return []
            
        symbol = tick_data.get('symbol')
        if not symbol:
            return []
            
        # Initialize data structures if needed
        self._initialize_symbol_data(symbol)
        
        # Get current timestamp
        current_time = time.time()
        
        # Extract price and volume
        price = tick_data.get('price')
        volume = tick_data.get('volume', 0)
        
        if price is None:
            return []
            
        completed_timeframes = []
        
        # Update each timeframe
        for timeframe, seconds in self.timeframes.items():
            if self._should_update_timeframe(symbol, timeframe, current_time):
                data = self.aggregated_data[symbol][timeframe]
                
                # If this is the first tick in the timeframe
                if data['count'] == 0:
                    data['open'] = price
                    data['high'] = price
                    data['low'] = price
                    data['close'] = price
                    data['volume'] = volume
                    data['count'] = 1
                else:
                    # Update existing timeframe data
                    data['high'] = max(data['high'], price)
                    data['low'] = min(data['low'], price)
                    data['close'] = price
                    data['volume'] += volume
                    data['count'] += 1
                
                # If we have completed a timeframe, prepare the data for QuestDB
                if data['count'] > 0:
                    completed_data = {
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'open': data['open'],
                        'high': data['high'],
                        'low': data['low'],
                        'close': data['close'],
                        'volume': data['volume'],
                        'timestamp': current_time,
                        'secType': tick_data.get('secType', 'STK'),
                        'exchange': tick_data.get('exchange', ''),
                        'country': tick_data.get('country', 'US')
                    }
                    completed_timeframes.append(completed_data)
                    
                    # Reset the timeframe data
                    self.aggregated_data[symbol][timeframe] = {
                        'open': None,
                        'high': None,
                        'low': None,
                        'close': None,
                        'volume': 0,
                        'count': 0
                    }
                    self.last_update[symbol][timeframe] = current_time
        
        return completed_timeframes 