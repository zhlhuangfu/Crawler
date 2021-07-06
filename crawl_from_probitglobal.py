import pdb
import json

from Modules.crawler import ProBitGlobalTradeDataCrawler


interval = 60
db_name = "xiehou_test"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
spider = ProBitGlobalTradeDataCrawler(db_name, interval, symbols)
spider.run()