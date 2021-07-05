import pdb
import json

from Modules.crawler import CoinbaseTradeDataCrawler


interval = 60
db_name = "test"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
spider = CoinbaseTradeDataCrawler(db_name, interval, symbols)
spider.run()