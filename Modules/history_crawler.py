import pdb
import json
import time
import datetime
import pytz

import requests
from .mysql_connector import CryptoCoinConnector
from .print_section import print_error_sleep, print_sleep, print_write_data


class BaseCrawler():
    """Base abstract of Crawler"""

    def __init__(self, db_name):
        self.connector = CryptoCoinConnector(db_name)
        
    def write_into_db(self, json_data):
        """Write infomation into DB"""
        raise NotImplementedError

    def request_data(self, *kwargs):
        raise NotImplementedError

    def _kernel(self, *kwargs):
        raise NotImplementedError

    def handle_network_issue(self, function, *kwargs):
        """Handle network issue"""
        FLAG = False
        for i in range(1, 60 * 10 + 1):
            if FLAG : break
            print_error_sleep(i)
            time.sleep(i)
            try:
                function(*kwargs)
                FLAG = True

            except:
                FLAG = False

    def run(self, test_flag=False):
        """Run crawler"""
        while True:
            try:
                self._kernel()
            except:
                self.handle_network_issue(self._kernel)
            print_sleep(self.interval)
            if test_flag:
                break
            time.sleep(self.interval)
        self.connector.close()


class BaseExchangeHistoryCrawler(BaseCrawler):
    def __init__(self, db_name, symbols, exch_name):
        super(BaseExchangeHistoryCrawler, self).__init__(db_name)
        self.start_timestamp = 1532398889
        self.end_timestamp = 1627006889
        self.exch_name = exch_name
        self.connector.create_meta_table()

        self.symbols = symbols
        data = []
        for symbol in symbols:
            utctick = int(time.time())
            data.append( (self.exch_name, symbol, utctick) )
            self.connector.create_trade_info_table(symbol)
        self.connector.insert_meta_data(data)
    
    def transform_symbol(self, symbol):
        return symbol
    
    def construct_url(self, symbol, start_timestamp, end_timestamp):
        return self.url.format(symbol, start_timestamp, end_timestamp)
    
    def request_data(self, url):
        response = requests.get(url)
        return json.loads(response.text)

    def parse_data(self, data, start_timestamp, end_timestamp):
        raise NotImplementedError
    
    def write_into_db(self, data_lst, symbol):
        self.connector.insert_trade_data(data_lst, symbol)

    def _kernel(self, symbol, start_timestamp, end_timestamp):
        q_symbol = self.transform_symbol(symbol)
        url = self.construct_url(q_symbol, start_timestamp, end_timestamp)
        data = self.request_data(url)
        data_lst, start_timestamp, end_timestamp, size = self.parse_data(data, start_timestamp, end_timestamp)
        self.write_into_db(data_lst, symbol)
        return start_timestamp, end_timestamp, size

    def run(self, test_flag=False):
        """Kernel function"""

        i = 1
        for symbol in self.symbols:
            start_timestamp = self.start_timestamp
            end_timestamp = self.end_timestamp
            while True:
                print_write_data()
                if i % self.rate_limit == 0:
                    time.sleep(self.interval)
                    print_sleep(self.interval)
                
            
                try:
                    start_timestamp, end_timestamp, size = self._kernel(symbol, start_timestamp, end_timestamp)
                    if size == 0:
                        break
                except:
                    self.handle_network_issue(self._kernel, symbol)
                
                if test_flag:
                    break

                i = i + 1

        self.connector.close()

class FTXUSHistoryTradeDataCrawler(BaseExchangeHistoryCrawler):
    def __init__(self, db_name, symbols):
        super(FTXUSHistoryTradeDataCrawler, self).__init__(db_name, symbols, "ftxus")
        self.url = "https://ftx.us/api/markets/{}/trades?start_time={}&end_time={}"
        self.rate_limit = 20
        self.interval = 1
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "/" + "USD"

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if res_data["success"] != True:
            return data_lst, start_timestamp, end_timestamp, 0

        for record in res_data["result"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["side"] == "buy" else False
            quoteQty = record["price"] * record["size"]
            time = record["time"][0:19]
            d = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S")
            ts = int(d.timestamp())
            data += [record["id"], record["price"], record["size"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)

        size = len(res_data["result"])
        if size != 0:
            # find the smallest timestamp to be the new end_timestamp
            new_end_time = res_data["result"][-1]["time"][0:19]
            d = datetime.datetime.strptime(new_end_time, "%Y-%m-%dT%H:%M:%S")
            end_timestamp = int(d.timestamp())

        return data_lst, start_timestamp, end_timestamp, size
