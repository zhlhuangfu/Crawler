import pdb
import json

from Modules.crawler import CoinDCXTradeDataCrawler


interval = 60
db_name = "trade_info"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
spider = CoinDCXTradeDataCrawler(db_name, interval, symbols)
spider.run()