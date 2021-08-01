import pdb
import json
import argparse

from Modules.history_crawler import FTXUSHistoryTradeDataCrawler


db_name = "xiehou_history_test"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
spider = FTXUSHistoryTradeDataCrawler(db_name, symbols)
spider.run()