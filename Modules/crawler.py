"""
It contains the class Crawler for crawling the price info,
CoinMarketCap API is used.
Along with the Crawler, there are other methods relate to use the API
"""
import pdb
import json
import time
import ccxt
import datetime

from .mysql_connector import CryptoCoinConnector
from .print_section import print_error_sleep, print_sleep, print_write_data


class BaseCrawler():
    """Base abstract of Crawler"""

    def __init__(self, db_name, interval):
        self.interval = interval
        self.connector = CryptoCoinConnector(db_name)
        
    def write_into_db(self, json_data):
        """Write infomation into DB"""
        raise NotImplementedError

    def request_data(self, *kwargs):
        raise NotImplementedError

    def process(self):
        raise NotImplementedError

    def handle_network_issue(self):
        """Handle network issue"""
        FLAG = False
        for i in range(1, 60 * 10 + 1):
            if FLAG : break
            print_error_sleep(i)
            time.sleep(i)
            try:
                self.process()
                FLAG = True

            except:
                FLAG = False

    def run(self):
        """Run crawler"""
        while True:
            try:
                self.process()
            except:
                self.handle_network_issue()
            print_sleep(self.interval)
            time.sleep(self.interval)
        self.connector.close()


class BaseExchangeCrawler(BaseCrawler):
    def __init__(self, db_name, interval, symbols, exch_name):
        super(BaseExchangeCrawler, self).__init__(db_name, interval)
        self.exch_name = exch_name
        self.connector.create_meta_table()
        self.exchange = getattr(ccxt, self.exch_name)()
        self.symbols = symbols
        data = []
        for symbol in symbols:
            utctick = int(time.time())
            data.append( (self.exch_name, symbol, utctick) )
            self.connector.create_trade_info_table(symbol)
        self.connector.insert_meta_data(data)
    
    def transform_symbol(self, symbols):
        raise NotImplementedError

    def process(self):
        """Kernel function"""
        print_write_data()
        try:
            for symbol in self.symbols:
                q_symbol = self.transform_symbol(symbol)
                data = self.request_data(q_symbol)
                self.write_into_db(data, symbol)
        except:
            raise ConnectionError

class BinanceTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BinanceTradeDataCrawler, self).__init__(db_name, interval, symbols, "binance")

    def transform_symbol(self, symbol):
        return symbol[:-4] + "/" + "USDT"

    def request_data(self, symbol):
        return self.exchange.fetch_trades(symbol, limit=1000)

    def write_into_db(self, res_data, symbol):
        data_lst = []
        for item in res_data:
            data = [self.exch_name]
            isBuyerMaker = True if item["side"] == "buy" else False
            data += [item["id"], item["price"], item["amount"], item["cost"], item["info"]["T"], isBuyerMaker]
            data_lst.append(tuple(data))
        self.connector.insert_trade_data(data_lst, symbol)
