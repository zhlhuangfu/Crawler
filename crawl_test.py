import pdb
import json

from Modules.crawler import BigONETradeDataCrawler


interval = 60
db_name = "xiehou_test"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
spider = BigONETradeDataCrawler(db_name, interval, symbols)
spider.run()