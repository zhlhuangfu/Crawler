import pdb
import json

from Modules.crawler import CryptoComExchangeTradeDataCrawler


interval = 60
db_name = "trade_info"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
spider = CryptoComExchangeTradeDataCrawler(db_name, interval, symbols)
spider.run()