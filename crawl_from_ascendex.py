import pdb
import json

from Modules.crawler import AscendEXTradeDataCrawler


interval = 60
db_name = "trade_info"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
spider = AscendEXTradeDataCrawler(db_name, interval, symbols)
spider.run()