import pdb
import json

from Modules.crawler import FTXUSTradeDataCrawler


interval = 60
db_name = "trade_info"
symbols = ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "UNIUSDT", "BCHUSDT"]
spider = FTXUSTradeDataCrawler(db_name, interval, symbols)
spider.run()