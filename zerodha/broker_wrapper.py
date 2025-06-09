import logging
import pandas as pd
import pytz
from datetime import datetime, timedelta
from kiteconnect import KiteConnect
from synapse.broker_aggregator.brokers.base import BaseBroker
import sqlite3




# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ZerodhaBroker(BaseBroker):
    def __init__(self):
        """Initialize the Zerodha broker client."""
        self.kite = None
        self.active_orders = {}
        self.magic = None  # For bot state persistence
        self.timeframe_map = {
            "1min": "minute",
            "5min": "5minute",
            "15min": "15minute",
            "30min": "30minute",
            "1hour": "60minute",
            "2hour": "60minute",  # Map to closest supported timeframe
            "4hour": "60minute",
            "6hour": "60minute",
            "12hour": "60minute",
            "1day": "day",
            "1week": "week",
            "1mnth": "month"
        }

    def connect(self, api_key='127.0.0.1', access_token='7497', client_id=None, timeout=10):
        """Connect to Zerodha Kite API."""
        if self.kite:
            self.disconnect()
        try:
            self.kite = KiteConnect(api_key=api_key)
            self.kite.set_access_token(access_token)
            print(f"Connected to Zerodha Kite API with api_key {api_key}")
        except Exception as e:
            print(f"Error connecting to Zerodha: {e}")
            raise

    def disconnect(self):
        """Disconnect from Zerodha API (no explicit disconnect in Kite API, clear session)."""
        self.kite = None
        print("Disconnected from Zerodha Kite API")

    def get_timeframe(self, timeframe):
        """Map internal timeframe to Kite Connect timeframe."""
        return self.timeframe_map.get(timeframe, "day")

    def fetch_data(self, symbol, start, end, timeframe):
        """Fetch historical data for a symbol."""
        try:
            kite_timeframe = self.get_timeframe(timeframe)
            instrument_token = self._get_instrument_token(symbol)
            diff = end - start
            days = max(diff.days, 1)  # At least 1 day
            duration_str = f"{days} D"
            data = self.kite.historical_data(
                instrument_token=instrument_token,
                from_date=start,
                to_date=end,
                interval=kite_timeframe
            )
            if not data:
                print(f"No data returned for {symbol} {duration_str} {timeframe}")
                return pd.DataFrame()
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df.rename(columns={
                'open': 'Open',
                'high': 'High',
                'low': 'Low',
                'close': 'Close',
                'volume': 'Volume'
            }, inplace=True)
            print(f"Fetched {len(df)} rows for {symbol}")
            return df
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()

    def _get_instrument_token(self, symbol):
        """Get instrument token for a symbol."""
        try:
            instruments = self.kite.instruments("NSE")
            for inst in instruments:
                if inst['tradingsymbol'] == symbol:
                    return inst['instrument_token']
            raise ValueError(f"Instrument not found for symbol: {symbol}")
        except Exception as e:
            print(f"Error fetching instrument token for {symbol}: {e}")
            raise

    def get_market_data(self, symbol):
        """Fetch latest market data for a symbol."""
        try:
            ltp_data = self.kite.ltp(f"NSE:{symbol}")
            market_data = {
                'last_price': ltp_data[f"NSE:{symbol}"]['last_price'],
                'timestamp': datetime.now(),
                'volume': ltp_data[f"NSE:{symbol}"].get('volume', 0)
            }
            return market_data
        except Exception as e:
            print(f"Error fetching market data for {symbol}: {e}")
            return {}

    def get_position_size(self, symbol):
        """Get total position size for a symbol."""
        positions = self.get_positions(symbol)
        size = 0
        for position in positions.to_dict('records'):
            if position["Symbol"] == symbol:
                size += position["Quantity"]
        return size

    def get_balance(self):
        """Get account balance."""
        try:
            margins = self.kite.margins("equity")
            balance = {
                'NetLiquidation': str(margins['net']),
                'AvailableFunds': str(margins['available']['cash']),
                'CashBalance': str(margins['available']['live_balance'])
            }
            return balance
        except Exception as e:
            print(f"Error fetching balance: {e}")
            return {}

    def get_current_time(self, symbol: str):
        """Get current time in US Eastern Time."""
        try:
            utc_time = datetime.utcnow()
            eastern = pytz.timezone('US/Eastern')
            eastern_time = utc_time.replace(tzinfo=pytz.utc).astimezone(eastern)
            return eastern_time.timestamp()
        except Exception as e:
            print(f"Error getting current time: {e}")
            raise

    def place_order(self, unique_id, symbol, volume, action, data, sl=None, tp=None, limit=None, stop=None, trailing=False):
        """Place a GTT order based on the signal."""
        timestamp = datetime.now().isoformat()
        try:
            print(f"\n[{timestamp}] Attempting to place order:")
            print(f"  Symbol: {symbol}, Volume: {volume}, SL:{sl}, TP:{tp}, Limit:{limit}, Stop:{stop}, Trailing:{trailing}")
            action = action.upper()
            if action not in ["BUY", "SELL"]:
                raise ValueError(f"Invalid action: {action}")
            tradingsymbol = symbol
            exchange = "NSE"
            transaction_type = self.kite.TRANSACTION_TYPE_BUY if action == "BUY" else self.kite.TRANSACTION_TYPE_SELL
            product = self.kite.PRODUCT_CNC
            quantity = abs(volume)
            last_price = data['Close'].iloc[-1] if data is not None and not data.empty else self.kite.ltp(f"{exchange}:{tradingsymbol}")[f"{exchange}:{tradingsymbol}"]['last_price']

            if trailing:
                print("Trailing stop-loss not supported with GTT orders")
                return None

            # Prepare GTT order parameters
            trigger_type = "two-leg" if sl and tp else "single" if sl or tp else None
            trigger_values = []
            order_params = []

            if trigger_type == "single":
                trigger_price = tp if action == "BUY" and tp else sl if action == "SELL" and sl else last_price
                limit_price = trigger_price + 0.5 if action == "BUY" else trigger_price - 0.5
                trigger_values.append(round(trigger_price, 2))
                order_params.append({
                    "exchange": exchange,
                    "tradingsymbol": tradingsymbol,
                    "transaction_type": transaction_type,
                    "order_type": self.kite.ORDER_TYPE_LIMIT,
                    "quantity": quantity,
                    "product": product,
                    "price": round(limit_price, 2)
                })
            elif trigger_type == "two-leg":
                sl_trigger = round(sl, 2)
                tp_trigger = round(tp, 2)
                trigger_values = [sl_trigger, tp_trigger]
                sl_limit = sl_trigger - 0.5 if action == "BUY" else sl_trigger + 0.5
                order_params.append({
                    "exchange": exchange,
                    "tradingsymbol": tradingsymbol,
                    "transaction_type": self.kite.TRANSACTION_TYPE_SELL if action == "BUY" else self.kite.TRANSACTION_TYPE_BUY,
                    "order_type": self.kite.ORDER_TYPE_LIMIT,
                    "quantity": quantity,
                    "product": product,
                    "price": round(sl_limit, 2)
                })
                tp_limit = tp_trigger + 0.5 if action == "BUY" else tp_trigger - 0.5
                order_params.append({
                    "exchange": exchange,
                    "tradingsymbol": tradingsymbol,
                    "transaction_type": self.kite.TRANSACTION_TYPE_SELL if action == "BUY" else self.kite.TRANSACTION_TYPE_BUY,
                    "order_type": self.kite.ORDER_TYPE_LIMIT,
                    "quantity": quantity,
                    "product": product,
                    "price": round(tp_limit, 2)
                })
            else:
                # Market order without SL/TP
                order_id = self.kite.place_order(
                    variety=self.kite.VARIETY_REGULAR,
                    exchange=exchange,
                    tradingsymbol=tradingsymbol,
                    transaction_type=transaction_type,
                    quantity=quantity,
                    product=product,
                    order_type=self.kite.ORDER_TYPE_MARKET
                )
                print(f"[{timestamp}] Market order placed: Order ID={order_id}")
                self.active_orders[order_id] = {
                    "symbol": symbol,
                    "action": action,
                    "quantity": quantity,
                    "sl": sl,
                    "tp": tp
                }
                return order_id

            # Place GTT order
            gtt_params = {
                "trigger_type": self.kite.GTT_TYPE_OCO if trigger_type == "two-leg" else self.kite.GTT_TYPE_SINGLE,
                "tradingsymbol": tradingsymbol,
                "exchange": exchange,
                "trigger_values": trigger_values,
                "last_price": round(last_price, 2),
                "orders": order_params
            }
            response = self.kite.place_gtt(**gtt_params)
            gtt_id = response.get('trigger_id')
            if gtt_id:
                print(f"[{timestamp}] GTT order placed: ID={gtt_id}")
                self.active_orders[gtt_id] = {
                    "symbol": symbol,
                    "action": action,
                    "quantity": quantity,
                    "sl": sl,
                    "tp": tp
                }
                return gtt_id
            else:
                print("Failed to place GTT order: No trigger ID returned")
                return None
        except Exception as e:
            print(f"Error placing order: {e}")
            return None

    def cancel_order(self, order_id):
        """Cancel a GTT or regular order."""
        try:
            if str(order_id) in self.active_orders and self.active_orders[str(order_id)].get('sl') or self.active_orders[str(order_id)].get('tp'):
                self.kite.delete_gtt(order_id)
                print(f"GTT order {order_id} canceled successfully.")
            else:
                self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR, order_id=order_id)
                print(f"Regular order {order_id} canceled successfully.")
            self.active_orders.pop(str(order_id), None)
        except Exception as e:
            print(f"Failed to cancel order {order_id}: {e}")

    def get_orders(self):
        """Fetch all orders (GTT and regular)."""
        try:
            gtts = self.kite.get_gtts()
            regular_orders = self.kite.orders()
            order_data = []
            for gtt in gtts:
                order_data.append({
                    "OrderId": gtt['id'],
                    "Symbol": gtt['condition']['tradingsymbol'],
                    "OrderType": "GTT",
                    "Quantity": gtt['orders'][0]['quantity'] if gtt['orders'] else 0,
                    "Status": gtt['status']
                })
            for order in regular_orders:
                if order['status'] in ['OPEN', 'TRIGGER PENDING']:
                    order_data.append({
                        "OrderId": order['order_id'],
                        "Symbol": order['tradingsymbol'],
                        "OrderType": order['order_type'],
                        "Quantity": order['quantity'],
                        "Status": order['status']
                    })
            return order_data
        except Exception as e:
            print(f"Error fetching orders: {e}")
            return []

    def get_open_orders(self):
        """Fetch all open orders (GTT and regular)."""
        return self.get_orders()

    def get_trade_history(self, start_time, end_time):
        """Fetch trade history using executed orders."""
        try:
            orders = self.kite.orders()
            trade_data = []
            for order in orders:
                if order['status'] == 'COMPLETE' and start_time <= pd.to_datetime(order['order_timestamp']) <= end_time:
                    trade_data.append({
                        "Trade ID": order['order_id'],
                        "Profit": order['average_price'] * order['quantity'],  # Approximate
                        "Date": pd.to_datetime(order['order_timestamp'])
                    })
            return trade_data
        except Exception as e:
            print(f"Error fetching trade history: {e}")
            return []

    def get_order_history(self, start_time, end_time):
        """Get order history (same as trade history)."""
        return self.get_trade_history(start_time, end_time)

    def get_positions(self, symbol):
        """Fetch current positions."""
        try:
            positions = self.kite.positions().get('net', [])
            positions_data = []
            for pos in positions:
                if symbol and pos['tradingsymbol'] != symbol:
                    continue
                order_type = "BUY" if pos['quantity'] > 0 else "SELL"
                positions_data.append({
                    "Order Ticket": pos['tradingsymbol'],
                    "Symbol": pos['tradingsymbol'],
                    "Type": order_type,
                    "Quantity": pos['quantity'],
                    "Price Open": pos['average_price'],
                    "Market Value": pos['value'],
                    "Unrealized PnL": pos['pnl'],
                    "Realized PnL": pos['realised']
                })
            df = pd.DataFrame(positions_data) if positions_data else pd.DataFrame()
            return df
        except Exception as e:
            print(f"Error fetching positions: {e}")
            return pd.DataFrame()

    def get_account_info(self):
        """Get account information."""
        try:
            margins = self.kite.margins("equity")
            account_info = [
                {"tag": "NetLiquidation", "value": margins['net']},
                {"tag": "AvailableFunds", "value": margins['available']['cash']},
                {"tag": "CashBalance", "value": margins['available']['live_balance']}
            ]
            return account_info
        except Exception as e:
            print(f"Error fetching account info: {e}")
            return []

    def get_all_trades(self, start_time, end_time):
        """Get all trades (same as trade history)."""
        return self.get_trade_history(start_time, end_time)

    def get_closed_trades(self, start_time, symbol):
        """Fetch closed trades for a symbol."""
        try:
            orders = self.kite.orders()
            trade_data = []
            for order in orders:
                if order['status'] == 'COMPLETE' and order['tradingsymbol'] == symbol and pd.to_datetime(order['order_timestamp']) >= start_time:
                    trade_data.append({
                        'ticket': order['order_id'],
                        'order': order['order_id'],
                        'time': pd.to_datetime(order['order_timestamp']),
                        'type': order['transaction_type'],
                        'price': order['average_price'],
                        'size': order['quantity'],
                        'commission': 0,  # Zerodha doesn't expose commission
                        'swap': 0,
                        'symbol': symbol
                    })
            df = pd.DataFrame(trade_data, columns=['ticket', 'order', 'time', 'type', 'price', 'size', 'commission', 'swap', 'symbol'])
            if df.empty:
                return pd.DataFrame()
            entry_trades = df[df['type'] == 'BUY']
            exit_trades = df[df['type'] == 'SELL']
            trade_book = []
            for _, entry in entry_trades.iterrows():
                matching_exits = exit_trades[exit_trades['order'] == entry['order']]
                if matching_exits.empty:
                    continue
                entry_time = entry['time']
                entry_price = entry['price']
                total_pnl = 0
                total_exit_volume = 0
                total_exit_price = 0
                for _, exit_trade in matching_exits.iterrows():
                    exit_volume = exit_trade['size']
                    exit_price = exit_trade['price']
                    pnl = (exit_price - entry_price) * exit_volume
                    if entry['type'] == 'SELL':
                        pnl *= -1
                    total_pnl += pnl
                    total_exit_volume += exit_volume
                    total_exit_price += exit_volume * exit_price
                return_pct = total_pnl / (entry_price * total_exit_volume) if total_exit_volume else 0
                duration = (matching_exits['time'].max() - entry_time).total_seconds()
                trade_book.append({
                    'EntryTime': entry_time,
                    'ExitTime': matching_exits['time'].max(),
                    'Ticker': symbol,
                    'Size': abs(total_exit_volume),
                    'EntryPrice': entry_price,
                    'ExitPrice': total_exit_price / total_exit_volume if total_exit_volume else 0,
                    'PnL': total_pnl,
                    'ReturnPct': return_pct,
                    'Duration': duration
                })
            return pd.DataFrame(trade_book)
        except Exception as e:
            print(f"Error fetching closed trades: {e}")
            return pd.DataFrame()

    def get_equity(self):
        """Get account equity (net liquidation value)."""
        balance = self.get_balance()
        return float(balance.get('NetLiquidation', 0))

    def get_free_margin(self):
        """Get available funds (free margin)."""
        balance = self.get_balance()
        return float(balance.get('AvailableFunds', 0))

    def close_position(self, symbol, fraction=1):
        """Close a position partially or fully."""
        positions = self.get_positions(symbol)
        for position in positions.to_dict('records'):
            if position['Symbol'] == symbol and position['Quantity'] != 0:
                volume = int(position['Quantity'] * fraction)
                transaction_type = self.kite.TRANSACTION_TYPE_SELL if position['Quantity'] > 0 else self.kite.TRANSACTION_TYPE_BUY
                try:
                    order_id = self.kite.place_order(
                        variety=self.kite.VARIETY_REGULAR,
                        exchange="NSE",
                        tradingsymbol=symbol,
                        transaction_type=transaction_type,
                        quantity=abs(volume),
                        product=self.kite.PRODUCT_CNC,
                        order_type=self.kite.ORDER_TYPE_MARKET
                    )
                    print(f"Closed {volume} of {symbol} position: Order ID={order_id}")
                except Exception as e:
                    print(f"Error closing position: {e}")

    def save_bot(self, bot_id):
        """Save bot state to SQLite database."""
        try:
            conn = sqlite3.connect("trading_bot.db")
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO bot_state (bot_id, magic_number)
                VALUES (?, ?)
            """, (bot_id, self.magic))
            conn.commit()
            conn.close()
            print(f"Saved bot state for bot_id={bot_id}")
        except Exception as e:
            print(f"Error saving bot state: {e}")

    def resume_bot(self, row):
        """Resume bot state from database row."""
        try:
            self.magic = row[6] if len(row) > 6 else None
            print(f"Resumed bot with magic_number={self.magic}")
        except Exception as e:
            print(f"Error resuming bot: {e}")