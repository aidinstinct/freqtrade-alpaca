# ![freqtrade](https://raw.githubusercontent.com/freqtrade/freqtrade/develop/docs/assets/freqtrade_poweredby.svg)

freqtrade-alpaca is a an open source fork of freqtrade for trading crypto-currencies and us equities. It currently supports Alpaca Markets and CCXT exchanges. 

## Currently supported features
 - Alpaca Markets
   - Backtesting
     1. Dynamic fee generation for crypto related trades.
     2. Extended hours trading (to mirror market conditions)
     3. Support to backtest both crypto and us equities in the same configuration
   - Data Management
     1. Downloads historic data from alpaca exchange
     2. Stores crypto data in the format "BTC_USD-USD-timeframe.feather"
   - Plotting
     1. Graphical support for Alpaca backtests
   - Exchange
     1. Alpaca Class instantiation
     2. Alpaca Websocket support
    - Hyperopt
      1. Support to optimize us equities and crypto on same configuration using Alpaca Markets
     
## *Note*
  1. Limit trading for Alpaca Markets is not thoroughly tested
  2. Options trading is not implemented
  3. Leverage trading is not implemented
  4. Orderbook insights are not implemented for crypto-currency trading on AlpacaExchange
  5. Simultaneous websockets(crypto endpoint, us-equities endpoint) supported
      1. Throws connection limit exceeded and will default to watching crypto (if trading crypto and us equities in same configuration)
      2. Requires multiple accounts (reference below for configuration)
  
 
## Disclaimer
This software is provided "as is," without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and noninfringement.

The creators, maintainers, and contributors of this fork assume no responsibility or liability for any use, misuse, or outcomes resulting from the use of this software. By using this software, you agree that you do so at your own risk.

Key points to note:

    This fork is provided solely for educational and informational purposes.
    The user is solely responsible for ensuring the software's suitability for their needs and compliance with all applicable laws and regulations.
    No guarantees are made regarding the functionality, accuracy, or reliability of the software.

By using this software, you acknowledge and accept these terms and waive any claim against the creators, maintainers, and contributors for any damages or losses that may occur, directly or indirectly, as a result of using this software.

## Documentation
The complete documentation on the [freqtrade website](https://www.freqtrade.io).

## Quick Start Guide (Using Alpaca Exchange)
Follow these steps to get started backtesting and trading on the Alpaca Exchange

Clone the fork
```
git clone https://github.com/aidinstinct/freqtrade-alpaca.git
```
Install 
```
cd freqtrade-alpaca
./setup.sh -i
```

Activate the enivronment
```
source .venv/bin/activate
```

Create a config for the bot using freqtrade cli
```
freqtrade new-config
```
## Make modifications to the generated config:

### Key configuration changes (for use with Alpaca Markets)

**important**
- **DO NOT**: do not put alpaca keys and secrets in exchange "key" or "secret".
- **DO NOT**: do not overload the api with a minimal `process_throttle_secs`.
- Keys/secret for live (`mkey`, `msecret`) and dry (`paper_key`, `paper_secret`).
- `extended_hours` parameter: specify whether to trade extended hours(before and after regular market).
- `account_id` parameter: Alpaca exchange account id.
- `max_open_trades`: Set to 3 if your account is less than $25,000 in value (due to day trading regulations).
- `process_throttle_secs`: Set to 40.
- `pair_whitelist` format:
  - Cryptocurrencies: `"BTC/USD/USD"`, `"ETH/USD/USD"`, ...
  - US equities: `"GLD/USD"`, `"SPY/USD"`, `"SIVR/USD"`.
## Example Configuration
```
{
    "$schema": "https://schema.freqtrade.io/schema.json",
    "max_open_trades": 3,
    "stake_currency": "USD",
    "stake_amount": 10,
    "extended_hours": false,
    "tradable_balance_ratio": 0.99,
    "fiat_display_currency": "USD",
    "timeframe": "30m",
    "dry_run": true,
    "dry_run_wallet": 1000,
    "cancel_open_orders_on_exit": false,
    "trading_mode": "spot",
    "margin_mode": "",
    "unfilledtimeout": {
        "entry": 10,
        "exit": 10,
        "exit_timeout_count": 0,
        "unit": "minutes"
    },
    "order_types": {
        "entry": "market",
        "exit": "market",
        "emergency_exit": "market",
        "force_entry": "market",
        "force_exit": "market",
        "stoploss": "market",
        "stoploss_on_exchange": false,
        "stoploss_on_exchange_interval": 60
    },
    "entry_pricing": {
        "price_side": "other",
        "use_order_book": false,
        "order_book_top": 1,
        "price_last_balance": 0.0,
        "check_depth_of_market": {
            "enabled": false,
            "bids_to_ask_delta": 1
        }
    },
    "exit_pricing":{
        "price_side": "other",
        "use_order_book": false,
        "order_book_top": 1
    },
    "exchange": {
        "name": "alpaca",
        "mkey": "your_live_key",
        "msecret": "your_live_secret",
        "paper_key": "your_paper_key",
        "paper_secret": "your_paper_secret",
        "paper_key_2": "your_paper_key_2",
        "paper_secret_2": "your_paper_secret_2",
        "account_id" : "your_account_id",
        "ccxt_config": {},
        "ccxt_async_config": {},
        "pair_whitelist": [
            "BTC/USD/USD",
            "AMD/USD",
            "DOGE/USD/USD",
            "ETH/USD/USD"
        ],
        "pair_blacklist": [
        ]
    },
    "pairlists": [
        {"method": "StaticPairList"}
    ],
    "telegram": {
        "enabled": false,
        "token": "",
        "chat_id": ""
    },
    "api_server": {
        "enabled": true,
        "listen_ip_address": "127.0.0.1",
        "listen_port": 8080,
        "verbosity": "error",
        "enable_openapi": false,
        "jwt_secret_key": "",
        "ws_token": "",
        "CORS_origins": [],
        "username": "",
        "password": ""
    },
    "bot_name": "freqtrade",
    "initial_state": "running",
    "force_entry_enable": false,
    "download_trades": false,
    "internals": {
        "process_throttle_secs": 40
    }
}
```

Generate a new strategy
```
freqtrade new-strategy
```

Download data
```
freqtrade download-data --config config.json --timerange 20240101- --timeframe 30m 1h 4h
```
Backtest
```
freqtrade backtesting --config config.json -s MyStrategy --timerange 20240101-
```

Hyperoptimize
```
freqtrade hyperopt --hyperopt-loss SharpeHyperOptLossDaily --config config.json -s MyStrategy --timerange 20240101-
```


## Branches
- `develop` - The primary development branch
- `main` - A tested branch

## Requirements
### Up-to-date clock

The clock must be accurate, synchronized to a NTP server very frequently to avoid problems with communication to the exchanges.

### Minimum hardware required (from freqtrade)

To run this bot we recommend you a cloud instance with a minimum of:

- Minimal (advised) system requirements: 2GB RAM, 1GB disk space, 2vCPU

### Software requirements (from freqtrade)
- [Python >= 3.10](http://docs.python-guide.org/en/latest/starting/installation/)
- [pip](https://pip.pypa.io/en/stable/installing/)
- [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
- [TA-Lib](https://ta-lib.github.io/ta-lib-python/)
- [virtualenv](https://virtualenv.pypa.io/en/stable/installation.html) (Recommended)
- [Docker](https://www.docker.com/products/docker) (Recommended)
