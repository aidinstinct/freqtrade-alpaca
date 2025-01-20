"""Alpaca exchange subclass"""
import asyncio
from copy import Error
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
import traceback
import re
from queue import Queue

import requests
import aiohttp
import pandas as pd
from freqtrade.enums import TradingMode, MarginMode
logger = logging.getLogger(__name__)
#need the live data socket:
class AlpacaWebSocket:
    def __init__(self, key_id, secret_key, config):
        self.key_id = key_id
        self.secret_key = secret_key
        self.config = config

        self.includes_crypto = self.config.get('includes_crypto', False)
        self.websocket_url = "wss://stream.data.alpaca.markets/v2/iex"
        self.crypto_websocket_url = "wss://stream.data.alpaca.markets/v1beta3/crypto/us"
        #self.websocket_url = "wss://data.alpaca.markets/stream"
        #self.websocket_url="wss://stream.data.alpaca.markets/v2/test"
        self.data = {}
        self.data_queue = Queue()  # Queue to store received data


        self.data_crypto = {}
        self.data_queue_crypto = Queue()  # Queue to store received data
        #self.websocket_url="wss://stream.data.alpaca.markets/v2/test"
        self.logger = logging.getLogger(__name__)
        self.connected = False  # Flag to indicate WebSocket connection status
        self.websocket = None  # To store the websocket object
        self.websocket_stocks = None  # To store the websocket object
        self.websocket_crypto = None  # To store the websocket object
        self.headers = {
            "APCA-API-KEY-ID": self.key_id,
            "APCA-API-SECRET-KEY": self.secret_key
        }
    def _process_data(self, data):
        """
        Process incoming data and store it in a structured format.
        """
        print(data)
        for item in data:
            symbol = item.get('S')
            if symbol:
                if symbol not in self.data:
                    self.data[symbol] = []
                self.data[symbol].append(item)
        
    def get_data(self) -> List[Dict]:
        """
        Get all available data
        Either from queue or self.data
        """
        print(self.data)
        return self.data

    async def stream_crypto(self, session_crypto):
        async with session_crypto.ws_connect(self.crypto_websocket_url) as websocket_crypto:
            try:
                print(f'Connected to {self.crypto_websocket_url}')
                self.websocket_crypto = websocket_crypto
                self.connected = True  # Set the connection status to True
                to_watch_crypto = []
                for pair in self.config["exchange"]["pair_whitelist"]:
                    parts = pair.split('/')
                    #its crypto
                    if len(parts) > 2:
                        base, quote = parts[0] + "/" + parts[1], parts[2]
                        to_watch_crypto.append(base)
                        print(to_watch_crypto)
                    
                #to_watch = [pair.split('/')[0] for pair in self.config["exchange"]["pair_whitelist"]]
                print('to watch:', to_watch_crypto)
                subscribe_message = {
                    "action": "subscribe",
                    "bars": to_watch_crypto
                }
                await websocket_crypto.send_json(subscribe_message)
                print("Subscribed to channels")
                self.data_queue_crypto.put("Subscribed to channels")
                logging.info("Subscribed to channels")

                async for message in websocket_crypto:
                    data_crypto = json.loads(message.data)
                    #organize by the symbol and append to the data dictionary
                    self.data_queue_crypto.put(data_crypto)  # Put received data into the queue
                    self._process_data(data_crypto)
            except Exception as e:
                print(e)
                self.connected=False

    async def stream_stocks(self, session_stock):
        async with session_stock.ws_connect(self.websocket_url) as websocket_stock:
            try:
                print(f'Connected to {self.websocket_url}')
                self.websocket = websocket_stock
                self.connected = True  # Set the connection status to True
                to_watch = []
                
                for pair in self.config["exchange"]["pair_whitelist"]:
                    parts = pair.split('/')
                    if len(parts) == 2:
                        base, quote = parts[0], parts[1]
                        to_watch.append(base)
                    
                #to_watch = [pair.split('/')[0] for pair in self.config["exchange"]["pair_whitelist"]]
                print('to watch:', to_watch)
                subscribe_message = {
                    "action": "subscribe",
                    "bars": to_watch
                }
                await websocket_stock.send_json(subscribe_message)
                print("Subscribed to channels")
                self.data_queue.put("Subscribed to channels")
                logging.info("Subscribed to channels")

                async for message in websocket_stock:
                    data = json.loads(message.data)
                    #organize by the symbol and append to the data dictionary
                    self.data_queue.put(data)  # Put received data into the queue
                    self._process_data(data)
            except Exception as e:
                print(e)
                self.connected=False

    async def _start_websocket(self):
        if self.connected:
            return  # If already connected, do nothing
        #create flags
        stocks_and_crypto = False
        has_crypto = False
        crypto_only = False
        has_stocks = False
        stocks_only = False
        for pair in self.config["exchange"]["pair_whitelist"]:
            parts = pair.split('/')
            #its crypto
            if len(parts) >= 2:
                #crypto included
                has_crypto = True
            if len(parts) == 2:
                has_stocks = True
        
        if has_crypto and has_stocks:
            #need both websockets
            stocks_and_crypto=True
        elif has_crypto and not has_stocks:
            crypto_only=True
        elif has_stocks and not has_crypto:
            stocks_only=True
        #need simultaneous websockets
        #it might throw an error that simultaneous websockets are open...
        if stocks_and_crypto:
            print('flag true')
            #crypto instance
            #need a seperate account to run simultaneous websockets
            cheaders = {
                "APCA-API-KEY-ID": self.config['exchange']['paper_key_2'],
                "APCA-API-SECRET-KEY": self.config['exchange']['paper_secret_2']
            }
            async with aiohttp.ClientSession(headers=cheaders) as session_crypto, aiohttp.ClientSession(headers=self.headers) as session_stock:
                crypto_task = asyncio.create_task(self.stream_crypto(session_crypto))
                stock_task = asyncio.create_task(self.stream_stocks(session_stock))
                await asyncio.gather(crypto_task, stock_task)

        if stocks_only:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.ws_connect(self.websocket_url) as websocket:
                    try:
                        print(f'Connected to {self.websocket_url}')
                        self.websocket = websocket
                        self.connected = True  # Set the connection status to True
                        to_watch = []
                        
                        for pair in self.config["exchange"]["pair_whitelist"]:
                            parts = pair.split('/')
                            if len(parts) == 2:
                                base, quote = parts[0], parts[1]
                                to_watch.append(base)
                            
                        #to_watch = [pair.split('/')[0] for pair in self.config["exchange"]["pair_whitelist"]]
                        print('to watch:', to_watch)
                        subscribe_message = {
                            "action": "subscribe",
                            "bars": to_watch
                        }
                        await websocket.send_json(subscribe_message)
                        print("Subscribed to channels")
                        self.data_queue.put("Subscribed to channels")
                        logging.info("Subscribed to channels")

                        async for message in websocket:
                            data = json.loads(message.data)
                            #organize by the symbol and append to the data dictionary
                            self.data_queue.put(data)  # Put received data into the queue
                            self._process_data(data)
                    except Exception as e:
                        print(e)
                        self.connected=False
        if crypto_only:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                async with session.ws_connect(self.crypto_websocket_url) as websocket_crypto: 
                    try:
                        print(f'Connected to {self.crypto_websocket_url}')
                        self.websocket_crypto = websocket_crypto
                        self.connected = True  # Set the connection status to True
                        to_watch_crypto = []
                        for pair in self.config["exchange"]["pair_whitelist"]:
                            parts = pair.split('/')
                            #its crypto
                            if len(parts) > 2:
                                base, quote = parts[0] + "/" + parts[1], parts[2]
                                to_watch_crypto.append(base)
                            
                        #to_watch = [pair.split('/')[0] for pair in self.config["exchange"]["pair_whitelist"]]
                        print('to watch:', to_watch_crypto)
                        subscribe_message = {
                            "action": "subscribe",
                            "bars": to_watch_crypto
                        }
                        await websocket_crypto.send_json(subscribe_message)
                        print("Subscribed to channels")
                        self.data_queue_crypto.put("Subscribed to channels")
                        logging.info("Subscribed to channels")

                        async for message in websocket_crypto:
                            data = json.loads(message.data)
                            #organize by the symbol and append to the data dictionary
                            self.data_queue_crypto.put(data)  # Put received data into the queue
                            self._process_data(data)
                    except Exception as e:
                        print(e)
                        self.connected=False
    
    async def close_websocket(self):
        if self.connected:
            if self.websocket:
                await self.websocket.close()
            if self.websocket_stocks:
                self.websocket_stocks.close()
            if self.websocket_crypto:
                self.websocker_crypto.close()
            self.connected = False
            self.logger.info("WebSocket closed")
class Alpaca:
    _params: Dict = {"trading_agreement": "agree"}
    _ft_has: Dict = {
        "stoploss_on_exchange": True,
        "stop_price_param": "stopLossPrice",
        "stop_price_prop": "stopLossPrice",
        "stoploss_order_types": {"limit": "limit", "market": "market"},
        "order_time_in_force": ["GTC", "IOC", "PO"],
        "ohlcv_candle_limit": 1000,
        "ohlcv_has_history": True,
        "trades_pagination": "id",
        "trades_pagination_arg": "since",
        "trades_pagination_overlap": False,
        "mark_ohlcv_timeframe": "4h",
    }

    _supported_trading_mode_margin_pairs: List[Tuple[TradingMode, MarginMode]] = [
        # TradingMode.SPOT always supported and not required in this list
        # (TradingMode.MARGIN, MarginMode.CROSS),
        # (TradingMode.FUTURES, MarginMode.CROSS)
    ]

    def __init__(self, config: Dict[str, Any]) -> None:
        #super().__init__(config)
        self._ft_has = {}  # This dictionary can hold overrides for exchange_has
        self.name = "alpaca"
        self.id = "alpaca"
        self.default_spread_percentage = 0.01
        self.base_url = "https://broker-api.alpaca.markets"
        self.paper_url = "https://paper-api.alpaca.markets"
        self.market_url = "https://data.alpaca.markets/v2/stocks/bars"
        self.market_url_ = "https://data.alpaca.markets"
        self.live_url = "https://api.alpaca.markets"
        self.paper_key = config['exchange']['paper_key']
        self.paper_secret = config['exchange']['paper_secret']
        self.market_key = config['exchange'].get('mkey', None)
        self.market_secret = config['exchange'].get('msecret', None)
        self.api_key = config['exchange']['key']
        self.api_secret = config['exchange']['secret']
        self.account_id = config['exchange']['account_id']

        self.headers = {
            'accept': 'application/json',
            'APCA-API-KEY-ID': self.paper_key,
            'APCA-API-SECRET-KEY': self.paper_secret,
        }
        self.mheaders = {
            'accept': 'application/json',
            'APCA-API-KEY-ID': self.paper_key,
            'APCA-API-SECRET-KEY': self.paper_secret
        }
        
    @property
    def precisionMode(self) -> List[str]:
        """Return a list of available timeframes supported by Alpaca"""
        # Define the mapping of your desired keys to Alpaca timeframes
        return 4
    @property
    def timeframes(self) -> dict:
        """Return a list of available timeframes supported by Alpaca"""
        # Define the mapping of your desired keys to Alpaca timeframes
        timeframe_mapping = {
            '1m': '1Min',
            '5m': '5Min',
            '15m': '15Min',
            '30m': '30Min',
            '1h': '1Hour',
            '4h' : '4Hour',
            '1d': '1Day',
            '1w': '1Week'
        }
        return timeframe_mapping
    def get_formatted_date(self,timestamp_ms: int) -> str:
        """
        Format timestamp to the required string format.
        """
        dt = datetime.utcfromtimestamp(timestamp_ms / 1000)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    def format_timeframe(self, timeframe: str) -> str:
        """
        Format the timeframe string to match Alpaca's API format.
        """
        timeframe_map = {
            "1Min": "1m",
            "5Min": "5m",
            "15Min": "15m",
            "30Min": "30m",
            "1Hour": "1h",
            "4Hour": "4h",
            "1Day": "1d"
        }
        return timeframe_map.get(timeframe, timeframe)
    def _format_timeframe(self, timeframe: str) -> str:
        """
        Format the timeframe string to match Alpaca's API format.
        """
        timeframe_map = {
            "1m": "1Min",
            "5m": "5Min",
            "15m": "15Min",
            "30m": "30Min",
            "1h": "1Hour",
            "4h": "4Hour",
            "1d": "1Day",
            "1w": "1Week"
        }
        return timeframe_map.get(timeframe, timeframe)

    def exchange_has(self, endpoint: str) -> bool:
        """
        Checks if exchange implements a specific API endpoint.
        Wrapper around ccxt 'has' attribute
        :param endpoint: Name of endpoint (e.g. 'fetchOHLCV', 'fetchTickers')
        :return: bool
        """
        if hasattr(self, endpoint):
            if endpoint in self._ft_has.get("exchange_has_overrides", {}):
                return self._ft_has["exchange_has_overrides"][endpoint]
            return True
        else:
            return False

    def cancel_dry_order(self, order_id: str):
        """Function - cancel dry order""" 
        try:
            url = f"{self.paper_url}/v2/orders/{order_id}"
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.paper_key,
                "APCA-API-SECRET-KEY": self.paper_secret
            }
            response = requests.delete(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            return {}
        except requests.RequestException as e:
            raise Error(f"Error cancelling dry order: {str(e)}") from e
        
    def cancel_order(self, order_id: str):
        """Function  - cancel live order"""
        try:
            url = f"{self.market_url}/v2/orders/{order_id}"
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.market_key,
                "APCA-API-SECRET-KEY": self.market_secret
            }
            response = requests.delete(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            return {}
        except requests.RequestException as e:
            raise Error(f"Error cancelling order: {str(e)}") from e
    
    def close(self):
        """Function - placeholder for parent exchange.py"""
        pass

    def list_assets(self):
        """Function - list available alpaca assets"""
        try:
            #get crypto and us equities
            l=[]
            url = f"{self.paper_url}/v2/assets?asset_class=us_equity"
            response = requests.get(url, headers=self.mheaders, timeout=30)
            #print(f"Response Status Code: {response.status_code}")
            #print(f"Response Text: {response.text}")
            response.raise_for_status()  # Raise an error if the request was not successful
            l.extend(response.json())

            url = f"{self.paper_url}/v2/assets?asset_class=crypto"
            response = requests.get(url, headers=self.mheaders, timeout=30)
            #print(f"Response Status Code: {response.status_code}")
            #print(f"Response Text: {response.text}")
            response.raise_for_status()  # Raise an error if the request was not successful
            l.extend(response.json())
            return l
        except requests.RequestException as e:
            raise Error(f"Error fetching assets: {str(e)}") from e

    def load_markets(self):
        """Function - load markets for alpaca exchange"""
        alpaca_assets = self.list_assets()
        markets = {}
        try:
            for asset in alpaca_assets:
                if asset['status'] == 'active' and asset['tradable']:
                    symbol = asset['symbol']
                    markets_key = asset['symbol'] + "/" + "USD" if not asset['class'] == 'crypto' else asset['symbol'] + "/" + "USD"
                    quote = "USD"
                    base = symbol
                    min_trade_increment = asset.get('min_trade_increment', '0.000000001')  # Default value
                    min_order_size = asset.get('min_order_size', '1.0')  # Default value
                    price_increment = asset.get('price_increment', '0.01')  # Default value
                    if 'BTC' in markets_key:
                        print(markets_key)
                    markets[markets_key] = {
                        'id': asset['id'],
                        'asset_class' : asset['class'],
                        'lowercaseId': symbol.lower(),
                        'symbol': symbol if not asset['class'] =='crypto' else markets_key,
                        'base': base,
                        'quote': quote,
                        'settle': None,
                        'baseId': base,
                        'quoteId': quote,
                        'settleId': None,
                        'type': 'spot',
                        'spot': True,
                        'margin': False,
                        'swap': False,
                        'future': False,
                        'option': False,
                        'index': False,
                        'active': True,
                        'contract': False,
                        'linear': None,
                        'inverse': None,
                        'subType': None,
                        'taker': 0.000,  # No commission fee alpaca 2024
                        'maker': 0.000,  # No commission fee alpaca 2024
                        'contractSize': None,
                        'expiry': None,
                        'expiryDatetime': None,
                        'strike': None,
                        'optionType': None,
                        'precision': {
                            'amount': float(min_trade_increment),
                            'price': float(price_increment),
                            'cost': None,
                            'base': None,
                            'quote': None
                        },
                        #for alpaca crypto
                        #Currently, an order (buy or sell) must not exceed $200k in notional. This is per an order.
                        'limits': {
                            'leverage': {'min': 1, 'max': 1},
                            'amount': {'min': float(min_order_size), 'max': None},
                            'price': {'min': float(price_increment), 'max': None},
                            'cost': {'min': 0 if asset['class'] == 'crypto' else None, 'max': 200000 if asset['class'] == 'crypto' else None}
                        },
                        'created': None,
                        'info': {
                            'altname': symbol,
                            'wsname': symbol,
                            'aclass_base': 'currency',
                            'base': base,
                            'aclass_quote': 'currency',
                            'quote': quote,
                            'lot': 'unit',
                            'cost_decimals': 2,
                            'pair_decimals': 2,
                            'lot_decimals': 8,
                            'lot_multiplier': 1,
                            'leverage_buy': [],
                            'leverage_sell': [],
                            'fees': [['0', '0.0']],  # Example fees
                            'fees_maker': [['0', '0.0']],  # Example fees
                            'fee_volume_currency': 'USD',
                            'margin_call': None,
                            'margin_stop': None,
                            'ordermin': float(min_order_size),
                            'costmin': float(price_increment),
                            'tick_size': float(price_increment),
                            'status': 'online'
                        },
                        'percentage': True,
                        'tierBased': False,
                        'tiers': {'taker': [[0.0, 0.000]], 'maker': [[0.0, 0.000]]}
                    }
            return markets
        except Exception as e:
            raise Error from e

    def calculate_start_time_from_sincems(self, timeframe: str, sincems: int) -> str:
        """Function - calculate start time from ms for fetch ohlcv and get_historic_ohlcv"""
        # Convert sincems (milliseconds) to seconds
        sincems_seconds = sincems / 1000
        # Calculate the start time as a datetime object
        start_datetime = datetime.fromtimestamp(sincems_seconds, tz=timezone.utc)
        # Get the interval in seconds for the given timeframe
        interval_seconds = self.timeframe_to_seconds(timeframe)
        if interval_seconds is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
        # Adjust start time based on the timeframe
        # Subtract the remainder of the current time divided by the timeframe
        adjusted_start_time = start_datetime - timedelta(seconds=start_datetime.second % interval_seconds)
        # Format the adjusted start time into ISO 8601 format
        start_time_str = adjusted_start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        return start_time_str

    def calculate_start_time(self, timeframe: str, limit: int) -> str:
        """Function - calculate the start time for fetch ohlcv and get_historic ohlcv functions"""
        # Get the interval in seconds based on the timeframe
        interval_seconds = self.timeframe_to_seconds(timeframe)
        if interval_seconds is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        # Get the current time in UTC
        current_time = datetime.now(timezone.utc)

        # Calculate the start time by subtracting the time for the given limit
        time_difference = timedelta(seconds=interval_seconds * limit)
        start_time = current_time - time_difference

        # Format the start time in ISO 8601 format
        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        return start_time_str

    def get_alpaca_server_time(self):
        """Function - retrieve alpaca exchange server time"""
        try:
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.market_key,
                "APCA-API-SECRET-KEY": self.market_secret
            }
            response = requests.get("https://data.alpaca.markets/v2/clock", headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            return data['timestamp']
        except requests.RequestException as e:
            raise Error(f"Error fetching Alpaca server time: {str(e)}") from e

    def timeframe_to_seconds(self,timeframe: str) -> int:
        """Function - timeframe to seconds"""
        units_in_seconds = {
            's': 1,
            'm': 60,
            'min': 60,
            'hour': 3600,
            'h': 3600,
            'd': 86400,
            'day': 86400,
            'w': 604800,
            'week': 604800,
            'M': 2628000,  # Assuming a month is 30.44 days
            'y': 31536000,
        }

        # Match numeric value with the unit (e.g., "30Min", "1h", "2D")
        match = re.match(r"(\d+)([a-zA-Z]+)", timeframe)
        if not match:
            raise ValueError(f"Invalid timeframe format: {timeframe}")

        value, unit = match.groups()
        value = int(value)
        unit = unit.lower()

        if unit not in units_in_seconds:
            raise ValueError(f"Unknown timeframe unit: {unit}")

        return value * units_in_seconds[unit]

    def check_intervals(self,filled_bars_data, interval_ms):
        """Function - check intervals"""
        missing_intervals = []
        last_timestamp = None

        for item in filled_bars_data:
            current_timestamp = item['t']

            if last_timestamp:
                # Check for any gaps
                while last_timestamp + interval_ms < current_timestamp:
                    last_timestamp += interval_ms
                    missing_intervals.append(last_timestamp)
            last_timestamp = current_timestamp
        return missing_intervals
    
    def timeframe_to_minutes(self, timeframe):
        """Function - converts timeframe to minutes"""
        # Mapping of time units to their equivalent in minutes
        time_mapping = {
            'Min': 1,
            'Hour': 60,
            'Day': 1440,
            'Week': 10080
        }
        # Extract the numeric part and the unit part
        for unit, t in time_mapping.items():
            if timeframe.endswith(unit):
                num = int(timeframe.replace(unit, ''))
                return num * t
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    def check_missing_intervals(self,filled_bars_data, interval_minutes) -> bool:
        interval_ms = interval_minutes * 60 * 1000  # Convert minutes to milliseconds
        missing_intervals = self.check_intervals(filled_bars_data, interval_ms)

        if missing_intervals:
            print("Missing intervals found")
            for timestamp in missing_intervals:
                print(f"Missing interval at: {datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')}")
            return missing_intervals
        else:
            print("No missing intervals found. All intervals are accounted for.")
            return []

    def generate_missing_candles(self, bars_data, symbol, timeframe):
        """Function - Generate missing candles, used by fetch ohlcv for after market hours"""
        #print(f'the length of the bars data from api return {len(bars_data)}')
        if not bars_data:
            return []
        
        formatted_timeframe = self._format_timeframe(timeframe)
        interval_seconds = self.timeframe_to_seconds(formatted_timeframe)

        filled_bars_data = []
        last_timestamp = None

        for item in bars_data:
            if last_timestamp:
                current_timestamp = item['t']
                while last_timestamp + interval_seconds * 1000 < current_timestamp:
                    last_timestamp += interval_seconds * 1000
                    filled_bars_data.append({
                        't': last_timestamp,
                        'o': last_close,
                        'h': last_close,
                        'l': last_close,
                        'c': last_close,
                        'v': 0
                    })

            filled_bars_data.append(item)
            last_timestamp = item['t']
            last_close = item['c']

        # Check if we need to fill candles up to the current time or market close
        market_hours = self.check_market_hours()
        if market_hours['is_open']:
            #just return the data
            return filled_bars_data

        current_time = datetime.now(timezone.utc).timestamp() * 1000

        # Calculate the nearest previous interval of the current time
        #last_interval_time = current_time - (current_time % (interval_seconds * 1000))
        # If last_candle_time is the last interval before the current time, skip filling
        #if within market hours

        # Define market open and close times
        #market_open_time =
        #market_close_time =

        #if last_candle_time + (interval_seconds * 1000) >= last_interval_time:
        #    print('returning data without filling last interval time, letting aggregate function do it')
        #    return filled_bars_data
        #good for fetch_ohlcv, bad for historic.
        final_timestamp = current_time

        while last_timestamp + interval_seconds * 1000 < final_timestamp:
            last_timestamp += interval_seconds * 1000
            filled_bars_data.append({
                't': last_timestamp,
                'o': last_close,
                'h': last_close,
                'l': last_close,
                'c': last_close,
                'v': 0
            })
        #print(f'the length of the bars data from api return after fill {len(filled_bars_data)}')
        #interval_minutes = self.timeframe_to_minutes(timeframe)
        return filled_bars_data

    def fetch_alpaca_ticker_crypto(
        self,
        symbol: str,
        params: Dict[str, Any] = None #default to None
    ):
        """Function - fetch crypto ticker from alpaca api"""
        try:
            url = self.market_url_ + f'/v1beta3/crypto/us/latest/quotes?symbols={str(symbol).replace("/", "%2F")}'

            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.paper_key,
                "APCA-API-SECRET-KEY": self.paper_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            #also retrieve the latest candle to compare bp and ap to ohlcv
            #returns bid and ask
            quote = data.get('quotes', {})
            quote_data = quote[symbol]
            bid_price = quote_data.get('bp', 0)
            ask_price = quote_data.get('ap', 0)
            # Adjust ask price if it is 0
            if ask_price == 0:
                if bid_price > 0:
                    ask_price = bid_price * (1 + self.default_spread_percentage)
                    logger.debug('Adjusted ask price using bid price: %s', ask_price)
                else:
                    logger.warning('Both bid price and ask price are 0. Cannot adjust ask price.')

            # Log the extracted bid and ask prices
            logger.debug('Extracted bid price: %s', bid_price)
            logger.debug('Extracted ask price: %s', ask_price)

            #now get the last price:
            url = self.market_url_ + f'/v1beta3/crypto/us/latest/trades?symbols={str(symbol).replace("/", "%2F")}'
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            trade = data.get('trades', {})
            trade_data = trade[symbol]
            last = trade_data.get('p')

            #we also need the open price for the latest interval
            url = self.market_url_ + f'/v1beta3/crypto/us/latest/bars?symbols={str(symbol).replace("/", "%2F")}'
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            bar = data.get('bars', {})
            bar_data = bar[symbol]
            #lets calculate this from the latest bars using the timeframe
            open_ = bar_data.get('o')
            low = bar_data.get('l')
            high = bar_data.get('h')
            close = bar_data.get('c')
            volume = bar_data.get('v')
            ticker = {
                'symbol': symbol,
                'bid': bid_price,
                'ask': ask_price,
                'last': last,
                'open': open_,
                'low': low,
                'high': high,
                'close': close,
                'volume': volume
            }

            return ticker
        except requests.RequestException as e:
            raise Error(f"Error fetching market price for symbol {symbol}: {str(e)}") from e

    def fetch_alpaca_ticker(
        self,
        symbol: str,
        params: dict[str, Any] = None #default to None
    ):
        """Function - fetch alpaca ticker for input symbol"""
        try:
            stock, _ = symbol.split('/')
            url = self.market_url_ + f'/v2/stocks/{stock}/quotes/latest'
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.paper_key,
                "APCA-API-SECRET-KEY": self.paper_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            quote = data.get('quote', {})
            bid_price = quote.get('bp', 0)
            ask_price = quote.get('ap', 0)
            # Adjust ask price if it is 0
            if ask_price == 0:
                if bid_price > 0:
                    ask_price = bid_price * (1 + self.default_spread_percentage)
                    logger.debug('Adjusted ask price using bid price: %s', ask_price)
                else:
                    logger.warning('Both bid price and ask price are 0. Cannot adjust ask price.')

            # Log the extracted bid and ask prices
            logger.debug('Extracted bid price: %s', bid_price)
            logger.debug('Extracted ask price: %s', ask_price)

            url = self.market_url_ + f'/v2/stocks/{stock}/trades/latest'
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            trade = data.get('trade', {})
            last = trade.get('p')
            #we also need the open price for the latest interval
            url = self.market_url_ + f'/v2/stocks/{stock}/bars/latest'
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            bar_ = data.get('bar', {})

            #lets calculate this from the latest bars using the timeframe
            open_ = bar_.get('o')
            low = bar_.get('l')
            high = bar_.get('h')
            close = bar_.get('c')
            volume = bar_.get('v')

            ticker = {
                'symbol': symbol,
                'bid': bid_price,
                'ask': ask_price,
                'last': last,
                'open': open_,
                'low': low,
                'high': high,
                'close': close,
                'volume': volume
            }

            return ticker
        except requests.RequestException as e:
            raise Error(f"Error fetching market price for symbol {symbol}: {str(e)}") from e

    def check_market_hours(self):
        """Function - check market hours"""
        try:
            url = self.paper_url + "/v2/clock"
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.paper_key,
                "APCA-API-SECRET-KEY": self.paper_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code in (200, 201):

                data = response.json()
                return data
            else:
                return None
        except requests.RequestException as e:
            raise Error(f"Error fetching market hours: {str(e)}") from e

    def check_order_status(self, order_id: str):
        """Function - Check live order status"""
        try:
            url = f"{self.live_url}/v2/orders/{order_id}"
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.market_key,
                "APCA-API-SECRET-KEY": self.market_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            return data
        except requests.RequestException as e:
            raise Error(f"Error fetching order status for order ID {order_id}: {str(e)}") from e

    def check_paper_order_status(self, order_id: str):
        """Function - Check paper(dry) order status"""
        try:
            url = f"{self.paper_url}/v2/orders/{order_id}"
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.paper_key,
                "APCA-API-SECRET-KEY": self.paper_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            return data
        except requests.RequestException as e:
            raise Error(f"Error fetching order status for order ID {order_id}: {str(e)}") from e

    def fetch_dry_daytrade_count(self):
        """Function - Fetch dry(paper) day trade account"""
        try:
            #ambiguous findings in the url prepending "papertrader" if a forward slash is included in the url
            #<a href="/papertrader/api/v2/account">Moved Permanently</a>.  if there is a forward slash
            url = self.paper_url + "/v2/account"
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.paper_key,
                "APCA-API-SECRET-KEY": self.paper_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error for bad responses
            data = response.json()
            return data.get('daytrade_count', 0)  # Adjust based on actual API response structure
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e.response.status_code} - {e.response.reason}")
            print(f"Response Content: {e.response.text}")
            raise Error(f"Error fetching day trade count: {str(e)}") from e
        except Exception as e:
            print(f"Unexpected Error: {str(e)}")
            raise Error(f"Error fetching day trade count: {str(e)}") from e
    
    def fetch_daytrade_count(self):
        """Function - Fetch live daytrade count"""
        try:
            url = self.live_url + "/v2/account"         
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.market_key,
                "APCA-API-SECRET-KEY": self.market_secret
            }
            response = requests.get(url, headers=headers,timeout=30)
            response.raise_for_status()  # Raise an error for bad responses
            data = response.json()
            return data.get('daytrade_count', 0)  # Adjust based on actual API response structure
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error: {e.response.status_code} - {e.response.reason}")
            print(f"Response Content: {e.response.text}")
            raise Error(f"Error fetching day trade count: {str(e)}") from e
        except Exception as e:
            print(f"Unexpected Error: {str(e)}")
            raise Error(f"Error fetching day trade count: {str(e)}") from e

    def fetch_dry_balance(self):
        """Function - fetch dry(paper) account balance"""
        try:
            url = self.paper_url + "/v2/account"
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.paper_key,
                "APCA-API-SECRET-KEY": self.paper_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            balances = {}
            free = float(response.json().get('cash'))
            equity = float(response.json().get('equity'))
            used = float(equity) - float(free)
            #set total as equity
            total = equity
            balances['USD'] = {
                'free': free,
                'used': used,
                'total': total
            }

            #get the remaining balances from positions
            positions = self.fetch_dry_positions([])
            for position in positions:
                symbol = position.get('symbol')
                free = float(position.get('qty_available'))
                used = float(0.0)
                total = float(position.get('qty'))

                balances[symbol] = {
                    'free': free,
                    'used': used,
                    'total': total
                }

            return balances

        except requests.RequestException as e:
            raise Error(f"Error fetching account balance: {str(e)}") from e

    def fetch_balance(self):
        """Function -  fetch live balance"""
        try:
            url = self.live_url + "/v2/account/"
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.market_key,
                "APCA-API-SECRET-KEY": self.market_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            balances = {}
            free = float(response.json().get('cash'))
            #calculate used by subtracting the cash from the equity
            equity = float(response.json().get('equity'))
            used = float(equity) - float(free)
            #set total as equity
            total = equity
            balances['USD'] = {
                'free': free,
                'used': used,
                'total': total
            }
            #get the remaining balances from positions
            positions = self.fetch_positions([])
            for position in positions:
                symbol = position.get('symbol')
                free = float(position.get('qty_available'))
                used = float(0.0)
                total = float(position.get('qty'))
                balances[symbol] = {
                    'free': free,
                    'used': used,
                    'total': total
                }

            return balances

        except requests.RequestException as e:
            raise Error(f"Error fetching account balance: {str(e)}") from e

    def fetch_positions(self, symbols):
        """Function - fetch live positions (market/live account)"""
        try:
            if symbols:
                for symbol in symbols:
                    symbol_, _ = symbol.split('/')
                    url = self.live_url + f"/v2/positions/{symbol_}"
                    headers = {
                        "Accept": "application/json",
                        "APCA-API-KEY-ID": self.market_key,
                        "APCA-API-SECRET-KEY": self.market_secret
                    }
                    response = requests.get(url, headers=headers, timeout=30)
                    response.raise_for_status()  # Raise an error if the request was not successful
                    data = response.json()
                    return data
            else:
                url = self.live_url + "/v2/positions"
                headers = {
                    "Accept": "application/json",
                    "APCA-API-KEY-ID": self.market_key,
                    "APCA-API-SECRET-KEY": self.market_secret
                }
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()  # Raise an error if the request was not successful
                data = response.json()
                return data

        except requests.RequestException as e:
            raise Error(f"Error fetching account positions: {str(e)}") from e

    def fetch_dry_positions(self, symbols):
        """Function - fetch dry(paper account) positions"""
        try:
            if symbols:
                for symbol in symbols:
                    symbol_, _ = symbol.split('/')
                    url = self.paper_url + f"/v2/positions/{symbol_}"
                    headers = {
                        "Accept": "application/json",
                        "APCA-API-KEY-ID": self.paper_key,
                        "APCA-API-SECRET-KEY": self.paper_secret
                    }
                    response = requests.get(url, headers=headers, timeout=30)
                    response.raise_for_status()  # Raise an error if the request was not successful
                    data = response.json()
                    return data
            else:
                #return all positions
                url = self.paper_url + "/v2/positions"
                headers = {
                    "Accept": "application/json",
                    "APCA-API-KEY-ID": self.paper_key,
                    "APCA-API-SECRET-KEY": self.paper_secret
                }
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()  # Raise an error if the request was not successful
                data = response.json()
                return data
        except requests.RequestException as e:
            raise Error(f"Error fetching account positions: {str(e)}") from e

    def create_paper_order_extended(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        rate: float,
        leverage: int=1,
        params: Dict[str, Any] = None,
        stop_loss: bool = False,
    ) -> None:
        """Function - Create paper order for extended hours"""
        try:
            time_in_force = "day" if isinstance(amount, float) else "gtc"
            parts = symbol.split('/')
            is_crypto = False
            if len(parts) ==2:
                #its equity
                is_crypto = False
            if len(parts) > 2:
                is_crypto = True         
            if is_crypto:
                time_in_force = "ioc"
                symbol = parts[0] + '/' + parts[-1]
            print(symbol, order_type, side, amount, rate, leverage, params, stop_loss, time_in_force)
            if order_type == 'market':
                url = self.paper_url + "/v2/orders"
                data = {
                    "symbol": symbol,
                    "qty": amount,
                    "side": side,
                    "type": order_type,
                    "time_in_force": time_in_force,
                    "extended_hours": True
                }
                headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json",  # Specifies the format of the request payload
                    "APCA-API-KEY-ID": self.paper_key,
                    "APCA-API-SECRET-KEY": self.paper_secret
                }
                response = requests.post(url, json=data, headers=headers, timeout=30)

                # Raise an error for HTTP status codes other than 2xx
                response.raise_for_status()  # Raise an error if the request was not successful
                return response.json()

            elif order_type == 'limit':
                url = self.paper_url + "/v2/orders"
                data = {
                    "symbol": symbol,
                    "qty": float(amount),
                    "side": side,
                    "type": order_type,
                    "limit_price": rate,
                    "time_in_force": time_in_force,
                    "extended_hours": True
                }
                #print(data)
                headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json",  # Specifies the format of the request payload
                    "APCA-API-KEY-ID": self.paper_key,
                    "APCA-API-SECRET-KEY": self.paper_secret
                }
                response = requests.post(url, json=data, headers=headers, timeout=30)
                response.raise_for_status()
                return response.json()

        except Exception as e:
            raise Error(f"Error creating order: {str(e)}") from e

    def create_paper_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        rate: float,
        leverage: int=1,
        params: Dict[str, Any] = None,
        stop_loss: bool = False,
    ):
        """Function - create alpaca paper order"""
        try:
            time_in_force = "day" if isinstance(amount, float) else "gtc"
            parts = symbol.split('/')
            is_crypto = False
            if len(parts) ==2:
                #its equity
                is_crypto = False
            if len(parts) > 2:
                is_crypto = True     
            if is_crypto:
                time_in_force = "ioc"
                symbol = parts[0] + '/' + parts[-1]            
            #print(symbol, order_type, side, amount, rate, leverage, params, stop_loss, time_in_force)
            if order_type == 'market':
                url = self.paper_url + "/v2/orders"
                data = {
                    "symbol": symbol,
                    "qty": amount,
                    "side": side,
                    "type": order_type,
                    "time_in_force": time_in_force
                }
                headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json",  # Specifies the format of the request payload
                    "APCA-API-KEY-ID": self.paper_key,
                    "APCA-API-SECRET-KEY": self.paper_secret
                }
                response = requests.post(url, json=data, headers=headers, timeout=30)
                # Raise an error for HTTP status codes other than 2xx
                response.raise_for_status()  # Raise an error if the request was not successful
                return response.json()

            elif order_type == 'limit':
                url = self.paper_url + "/v2/orders"
                data = {
                    "symbol": symbol,
                    "qty": float(amount),
                    "side": side,
                    "type": order_type,
                    "limit_price": rate,
                    "time_in_force": time_in_force
                }
                #print(data)
                headers = {
                    "Accept": "application/json",
                    # Specifies the format of the request payload
                    "Content-Type": "application/json",
                    "APCA-API-KEY-ID": self.paper_key,
                    "APCA-API-SECRET-KEY": self.paper_secret
                }
                response = requests.post(url, json=data, headers=headers, timeout=30)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            raise Error(f"Error creating order: {str(e)}") from e

    def create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        amount: float,
        rate: float,
        leverage: int=1
    ):
        """Function - create alpaca live order"""
        try:
            #counting parts to distinguish crypto from us equity.
            parts = symbol.split('/')
            is_crypto = False
            if len(parts) ==2:
                #its equity
                is_crypto = False
            if len(parts) > 2:
                is_crypto = True           
            time_in_force = "day" if isinstance(amount, float) else "gtc"
            if is_crypto:
                time_in_force = "ioc"
                symbol = parts[0] + '/' + parts[-1]
            if order_type == 'market':
                url = self.live_url + "/v2/orders"
                data = {
                    "symbol": symbol,
                    "qty": amount,
                    "side": side,
                    "type": order_type,
                    "time_in_force": time_in_force
                }
                #logger.info(f'the data being sent its a market order {data}')
                headers = {
                    "Accept": "application/json",
                    # Specifies the format of the request payload
                    "Content-Type": "application/json",
                    "APCA-API-KEY-ID": self.market_key,
                    "APCA-API-SECRET-KEY": self.market_secret
                }
                response = requests.post(url, json=data, headers=headers, timeout=30)
                print(f"Response Status Code: {response.status_code}")
                print(f"Response Text: {response.text}")
                response.raise_for_status()  # Raise an error if the request was not successful
                return response.json()
            
            elif order_type == 'limit':
                url = self.live_url + "/v2/orders"
                #logger.info(f'{data}')
                data = {
                    "symbol": symbol,
                    "qty": amount,
                    "side": side,
                    "type": order_type,
                    "limit_price": rate,
                    "time_in_force": time_in_force
                }
                #print(data)
                headers = {
                    "Accept": "application/json",
                    "Content-Type": "application/json",  # Specifies the format of the request payload
                    "APCA-API-KEY-ID": self.market_key,
                    "APCA-API-SECRET-KEY": self.market_secret
                }
                response = requests.post(url, json=data, headers=headers, timeout=30)
                #print(f"Response Status Code: {response.status_code}")
                #print(f"Response Text: {response.text}")
                response.raise_for_status()
                return response.json()
        except Exception as e:
            raise Error(f"Error creating order: {str(e)}") from e

    def fetch_dry_orders(
        self,
        symbol,
        since_ms,
        params=None
    ):
        """Function - fetch alpaca paper orders"""
        try:
            # Get the orders for the specified symbol
            #since_iso8601 = datetime.datetime.fromtimestamp(since_ms / 1000.0).isoformat()
            since_dt = datetime.utcfromtimestamp(since_ms / 1000.0)
            since_iso8601 = since_dt.isoformat() + 'Z'  # Add 'Z' for UTC time
            url = f"{self.live_url}/v2/orders?status=all&direction=asc&after={since_iso8601}&symbols={symbol}"
            #retrieve the the orders using headers
            headers = {
                "Accept": "application/json",
                #does it need to be the market key?
                #i think so
                "APCA-API-KEY-ID": self.paper_key,
                "APCA-API-SECRET-KEY": self.paper_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            print(f"Response Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()

            #list of orders
            return data
        except Exception as e:
            raise Error(f"Error fetching orders {e}") from e

    async def fetch_orders(
        self,
        symbol,
        since_ms,
        params=None
    ):
        """Async Function - fetch alpaca live orders"""
        try:
            # Get the orders for the specified symbol
            #since_iso8601 = datetime.datetime.fromtimestamp(since_ms / 1000.0).isoformat()
            since_dt = datetime.utcfromtimestamp(since_ms / 1000.0)
            since_iso8601 = since_dt.isoformat() + 'Z'  # Add 'Z' for UTC time
            url = f"{self.live_url}/v2/orders?status=all&direction=asc&after={since_iso8601}&symbols={symbol}"
            #retrieve the the orders using headers
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.market_key,
                "APCA-API-SECRET-KEY": self.market_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            print(f"Response Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            orders = data.get('orders')
            return orders
        except Exception as e:
            raise Error(f"Error fetching orders for symbol: {str(e)}") from e

    def fetch_order(
        self,
        order_id: str,
    ):
        """Function - fetch alpaca live orders"""
        try:
            url = f"{self.live_url}/v2/orders/{order_id}"
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.market_key,
                "APCA-API-SECRET-KEY": self.market_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            return data
        except requests.RequestException as e:
            raise Error(f"Error fetching order for order ID {order_id}: {str(e)}") from e
        
    def fetch_dry_order(
            self,
            order_id: str,
    ):
        """Function - fetch alpaca dry(paper) order by order id"""
        try:
            url = f"{self.paper_url}/v2/orders/{order_id}"
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.paper_key,
                "APCA-API-SECRET-KEY": self.paper_secret
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()  # Raise an error if the request was not successful
            data = response.json()
            return data
        except requests.RequestException as e:
            raise Error(f"Error fetching order for order ID {order_id}: {str(e)}") from e
        
    async def fetch_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since: Optional[int],
        limit: int,
        params: Dict[str, Any] = None
    ):
        """Async Function - fetch ohlcv from trading bot, used in exchange.py klines"""
        try:
            formatted_timeframe = self._format_timeframe(timeframe)
            # Calculate the start time based on the candle limit
            start_time = self.calculate_start_time(timeframe, limit) if since is None else self.get_formatted_date(since)
            parts = pair.split('/')
            is_crypto = False
            if len(parts) > 2:
                is_crypto = True
                #its the crypto pair
                base, quote = parts[0] + "%2F" + parts[1], parts[2]
                start_time = start_time.replace(":", "%3A")
                url = f"https://data.alpaca.markets/v1beta3/crypto/us/bars?symbols={base}&timeframe={formatted_timeframe}&start={start_time}"
            elif len(parts)==2:
                base, quote = parts[0],parts[1]
                url = f"{self.market_url}?symbols={base}&timeframe={formatted_timeframe}&start={start_time}"
            #print(url)
            headers = {
                "Accept": "application/json",
                "APCA-API-KEY-ID": self.market_key,
                "APCA-API-SECRET-KEY": self.market_secret
            }

            all_bars = []
            next_page_token = None
            while True:
                if next_page_token:
                    if '&page_token=' in url:
                        url = url.split('&')
                        url[-1] = f"page_token={next_page_token}"
                        url = '&'.join(url)
                    else:
                        url += f"&page_token={next_page_token}"
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()  # Raise an error if the request was not successful
                data = response.json()

                if not data:
                    raise Error("No data returned from Alpaca API")
                if 'bars' not in data:
                    raise Error("No data returned from Alpaca API")

                base = base.replace('%2F', '/')
                bars_data = data['bars'][base]

                # Convert the timestamp format to milliseconds
                for item in bars_data:
                    # Remove the 'Z' from the end of the string
                    dt = datetime.fromisoformat(item['t'][:-1])
                    #dt_utc = pytz.utc.localize(dt)
                    dt_utc = dt.replace(tzinfo=timezone.utc)  # Set the timezone to UTC
                    item['t'] = int(dt_utc.timestamp() * 1000)
                all_bars.extend(bars_data)
                next_page_token = data.get('next_page_token')
                if not next_page_token:
                    break
            # Fill missing candles
            #we need to fill candles that are outside of the operating hours
            #check if the last candle time is in operating hours
            #if not fill the candles up to the current time
            all_bars = self.generate_missing_candles(all_bars, pair, formatted_timeframe)
            ohlcv_data = [
                [int(item['t']), item['o'], item['h'], item['l'], item['c'], item['v']]
                for item in all_bars
            ]
            #latest_bar = ohlcv_data[-1][0]
            #print("Last available candle time:", latest_bar)
            return ohlcv_data
        except requests.RequestException as e:
            raise Error(f"Error fetching OHLCV data for symbol {pair}: {str(e)}") from e

    def fill_missing_intervals(self, df, timeframe):
        """Function - fille missing intervals of ohlcv data"""
        # Convert the 't' column to datetime if it's not already
        df['t'] = pd.to_datetime(df['t'])
        # Ensure the DataFrame is sorted by timestamp
        df = df.sort_values(by='t')
        # Define the expected time delta based on the timeframe
        timeframe_to_timedelta = {
            '1m': timedelta(minutes=1),
            '5m': timedelta(minutes=5),
            '15m': timedelta(minutes=15),
            '30m': timedelta(minutes=30),
            '1h': timedelta(hours=1),
            '4h': timedelta(hours=4),
            '1d': timedelta(days=1),
        }
        expected_delta = timeframe_to_timedelta.get(timeframe)
        if expected_delta is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        filled_bars = []
        for i in range(1, len(df)):
            current_row = df.iloc[i]
            previous_row = df.iloc[i - 1]
            filled_bars.append(previous_row.to_dict())
            current_timestamp = current_row['t']
            previous_timestamp = previous_row['t']
            while current_timestamp - previous_timestamp > expected_delta:
                previous_timestamp += expected_delta
                filled_bars.append({
                    't': previous_timestamp,
                    'o': previous_row['o'],
                    'h': previous_row['h'],
                    'l': previous_row['l'],
                    'c': previous_row['c'],
                    'v': 0
                })

        # Add the last row
        filled_bars.append(df.iloc[-1].to_dict())

        # Convert back to DataFrame
        filled_df = pd.DataFrame(filled_bars).sort_values(by='t').reset_index(drop=True)
        return filled_df
    
    def check_missing_candles(self, df, timeframe):
        """Function - checking for missing candles in ohlcv data"""
        # Convert the 't' column to datetime if it's not already
        df['t'] = pd.to_datetime(df['t'])

        # Ensure the DataFrame is sorted by timestamp
        df = df.sort_values(by='t')

        # Define the expected time delta based on the timeframe
        timeframe_to_timedelta = {
            '1m': timedelta(minutes=1),
            '5m': timedelta(minutes=5),
            '15m': timedelta(minutes=15),
            '30m': timedelta(minutes=30),
            '1h': timedelta(hours=1),
            '4h': timedelta(hours=4),
            '1d': timedelta(days=1),
        }

        expected_delta = timeframe_to_timedelta.get(timeframe)
        if expected_delta is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        # Iterate through the DataFrame and check for missing candles
        missing_intervals = []
        for i in range(1, len(df)):
            current_timestamp = df.iloc[i]['t']
            previous_timestamp = df.iloc[i - 1]['t']
            if current_timestamp - previous_timestamp > expected_delta:
                missing_intervals.append((previous_timestamp, current_timestamp))

        if missing_intervals:
            print(f"Missing intervals found: {missing_intervals}")
            print("Filling them before storage")
            filled_data = self.fill_missing_intervals(df, timeframe)
            return filled_data
        else:
            print("No missing intervals found. All intervals are accounted for.")
            return df

        return missing_intervals
    
    def get_historic_ohlcv(
        self,
        pair: str,
        timeframe: str,
        since_ms: int,
        candle_type: str,
        is_new_pair: bool = False,
        until_ms: Optional[int] = None,
    ) -> List:
        """Function - Retrieve historic OHLCV data from Alpaca API."""
        formatted_timeframe = self._format_timeframe(timeframe)
        start = self.get_formatted_date(since_ms)
        if_ = datetime.now().strftime('%Y-%m-%d')
        try:
            #split the pair by / to get the base and quote
            parts = pair.split('/')
            limit=10000
            adjustment='raw'
            sort='asc'
            is_crypto = False
            if len(parts) > 2:
                is_crypto = True
                #its the crypto pair
                base, quote = parts[0] + "%2F" + parts[1], parts[2]
                #url = f"https://data.alpaca.markets/v1beta3/crypto/us/bars?symbols={symbol}&timeframe={formatted_timeframe}&start={start}&end={end}&limit={limit}&sort={sort}"
                url = f"https://data.alpaca.markets/v1beta3/crypto/us/bars?symbols={base}&timeframe={formatted_timeframe}&start={start}&limit={limit}&sort={sort}"
            else:
                base, quote = pair.split('/')
                url = f"{self.market_url}?symbols={base}&timeframe={formatted_timeframe}&start={start}&adjustment={adjustment}&sort={sort}"

            all_bars = []
            while True:
                #logger.info(f'theurl {url}')
                response = requests.get(url, headers=self.mheaders, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    the_key = f"{pair.split('/')[0]}/{str(quote)}" if is_crypto else base
                    bars = data['bars'].get(f"{the_key}", [])
                    all_bars.extend(bars)
                    next_page_token = data.get('next_page_token')
                    if next_page_token:
                        if is_crypto:
                            url = f"https://data.alpaca.markets/v1beta3/crypto/us/bars?symbols={base}&timeframe={formatted_timeframe}&start={start}&limit={limit}&sort={sort}&page_token={next_page_token}"
                        else:
                            url = f"{self.market_url}?symbols={base}&timeframe={formatted_timeframe}&start={start}&limit={limit}&adjustment={adjustment}&sort={sort}&page_token={next_page_token}"
                    else:
                        break
                else:
                    print(f"Failed to fetch data: {response.status_code} - {response.text}")
                    break
    
            # Convert the timestamp format to milliseconds
            for item in all_bars:
                dt = datetime.fromisoformat(item['t'][:-1])  # Remove the 'Z' from the end of the string
                #dt_utc = pytz.utc.localize(dt)
                dt_utc = dt.replace(tzinfo=timezone.utc)  # Set the timezone to UTC
                item['t'] = int(dt_utc.timestamp() * 1000)
            ohlcv_data = [
                [int(item['t']), item['o'], item['h'], item['l'], item['c'], item['v']]
                for item in all_bars
            ]
            print(len(ohlcv_data))
            return ohlcv_data

        except Exception as e:
            error_message = f"An error occurred: {str(e)}\n\nFull traceback:\n{traceback.format_exc()}"
            print(error_message)
            return []
    def get_stock_fee(self, taker_or_maker='taker'):
        """Function - Return stock fee for alpaca exchange"""
        return 0.0 #no fees alpaca 2024

    def get_alpaca_crypto_fee_tier(self, total_volume):
        """Function - Calculate crypto fee tier based on 30 day total_volume(usd)"""
        tier = None
        if 0 <= total_volume <= 100000:
            tier = {'maker': 0.0015, 'taker': 0.0025}
        elif 100000 < total_volume <= 500000:
            tier = {'maker': 0.0012, 'taker': 0.0022}
        elif 500000 < total_volume <= 1000000:
            tier = {'maker': 0.0010, 'taker': 0.0020}
        elif 1000000 < total_volume <= 10000000:
            tier = {'maker': 0.0008, 'taker': 0.0018}
        elif 10000000 < total_volume <= 25000000:
            tier = {'maker': 0.0005, 'taker': 0.0015}
        elif 25000000 < total_volume <= 50000000:
            tier = {'maker': 0.0002, 'taker': 0.0013}
        elif 50000000 < total_volume <= 100000000:
            tier = {'maker': 0.0002, 'taker': 0.0012}
        elif total_volume > 100000000:
            tier = {'maker': 0.0000, 'taker': 0.0010}
        else:
            raise ValueError("Invalid total_volume value. It must be a non-negative number.")
        if not tier:
            raise Error("total_volume out of range or invalid fee_tier table")
        return tier
    def get_fee_crypto(self, taker_or_maker='taker', dry=False, is_backtest=True):
        """
        Function - Return crypto fee for alpaca exchange
        Possibly used by backtest.py
        """
        if is_backtest:
            #use worst case trading fee
            return 0.0025
        if dry:
            headers = {
                'Accept': 'application/json',
                'Apca-Api-Secret-Key': self.paper_secret,
                'Apca-Api-Key-Id': self.paper_key
            }
        else:
            headers = {
                'Accept': 'application/json',
                'Apca-Api-Secret-Key': self.market_secret,
                'Apca-Api-Key-Id': self.market_key
            }

        #calculate the 30day trading volume from filled trades of the account

        # Calculate the start date (30 days ago)
        start_date = datetime.now() - timedelta(days=30)
        # Format it to ISO 8601 (date only)
        formatted_date = start_date.strftime("%Y-%m-%d")
        # If time is required, include it
        formatted_datetime = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        url = "https://api.alpaca.markets/v2/account/activities"
        params = {"activity_types": "FILL", "date": formatted_datetime}
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            activities = response.json()
            total_volume = sum(
                float(activity["qty"]) for activity in activities if activity["symbol"].endswith("USD")
            )
            print(f"30-day crypto trading volume: {total_volume}")
            def get_fee_tier(total_volume):
                tier = None
                if 0 <= total_volume <= 100000:
                    tier = {'maker': 0.0015, 'taker': 0.0025}
                elif 100000 < total_volume <= 500000:
                    tier = {'maker': 0.0012, 'taker': 0.0022}
                elif 500000 < total_volume <= 1000000:
                    tier = {'maker': 0.0010, 'taker': 0.0020}
                elif 1000000 < total_volume <= 10000000:
                    tier = {'maker': 0.0008, 'taker': 0.0018}
                elif 10000000 < total_volume <= 25000000:
                    tier = {'maker': 0.0005, 'taker': 0.0015}
                elif 25000000 < total_volume <= 50000000:
                    tier = {'maker': 0.0002, 'taker': 0.0013}
                elif 50000000 < total_volume <= 100000000:
                    tier = {'maker': 0.0002, 'taker': 0.0012}
                elif total_volume > 100000000:
                    tier = {'maker': 0.0000, 'taker': 0.0010}
                else:
                    raise ValueError("Invalid total_volume value. It must be a non-negative number.")
                if not tier:
                    raise Error("total_volume out of range or invalid fee_tier table")
                return tier
            return get_fee_tier(total_volume)

        else:
            logger.error("Failed to fetch trading fees from Alpaca: %s", response.status_code)
            return None

    def get_asset(self, symbol):
        """Function - get specified asset from alpaca exchange"""
        url = f"{self.paper_url}/v2/assets/{symbol}"
        headers = {
            "Accept": "application/json",
            "Apca-Api-Key-Id": self.api_key,
            "Apca-Api-Secret-Key": self.api_secret
        }
        response = requests.get(url, headers=headers, timeout=30)
        return response.json()

    def _set_leverage(
        self,
        leverage: float,
        pair: Optional[str] = None,
        accept_fail: bool = False,
    ):
        """
        not implemented.
        """
        return None