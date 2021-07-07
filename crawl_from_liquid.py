import pdb
import json

from Modules.crawler import LiquidTradeDataCrawler


interval = 60
db_name = "trade_info"
symbols = ["BTCUSDT", "ETHUSDT", "DOTUSDT", "UNIUSDT"]
spider = LiquidTradeDataCrawler(db_name, interval, symbols)
spider.run()