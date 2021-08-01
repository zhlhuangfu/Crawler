import pdb
import json
import argparse

from Modules.history_crawler import BinanceHistoryTradeDataCrawler


db_name = "history_trades"
symbols = ["BTC_USDT", "ETH_USDT", "BNB_USDT", "ADA_USDT", "DOGE_USDT", "XRP_USDT", "LTC_USDT", "DOT_USDT", "UNI_USDT", "BCH_USDT"]
spider = BinanceHistoryTradeDataCrawler(db_name, symbols)
spider.run()