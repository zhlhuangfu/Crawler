import pdb
import json

from Modules.crawler import OKExTradeDataCrawler


interval = 60
db_name = "trade_info"
symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
spider = OKExTradeDataCrawler(db_name, interval, symbols)
spider.run()