import pdb
import json

from Modules.crawler import BinanceTradeDataCrawler


interval = 60
db_name = "test"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
spider = BinanceTradeDataCrawler(db_name, interval, symbols)
spider.run()