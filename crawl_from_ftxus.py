import pdb
import json

from Modules.crawler import FTXUSTradeDataCrawler


interval = 60
db_name = "xiehou_test"
symbols = ["BTCUSDT", "ETHUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "UNIUSDT", "BCHUSDT"]
# symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
spider = FTXUSTradeDataCrawler(db_name, interval, symbols)
spider.run()