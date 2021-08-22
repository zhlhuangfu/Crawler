import pdb
import json
import time
import datetime
import pytz
from datetime import timezone

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
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp, exch_name):
        super(BaseExchangeHistoryCrawler, self).__init__(db_name)
        # self.start_timestamp = 1532398889
        # self.end_timestamp = 1627006889
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.exch_name = exch_name
        self.connector.create_meta_table()

        self.symbols = symbols
        data = []
        for symbol in symbols:
            utctick = int(time.time())
            data.append( (self.exch_name, symbol, utctick) )
            start = int(int(str(start_timestamp)[0:10]) / 2592000)
            end = int(int(str(end_timestamp)[0:10]) / 2592000)
            self.connector.create_trade_info_table(symbol, start, end)
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
        for record in data_lst:
            timestamp = int(record[5])
            table_index = int(timestamp / 2592000)
            data = []
            data.append(record)
            self.connector.insert_trade_data(data, symbol, table_index)

    def _kernel(self, symbol, start_timestamp, end_timestamp):
        q_symbol = self.transform_symbol(symbol)
        url = self.construct_url(q_symbol, start_timestamp, end_timestamp)
        data = self.request_data(url)
        data_lst, start_timestamp, end_timestamp, size = self.parse_data(data, start_timestamp, end_timestamp)
        
        new_start = int(str(start_timestamp)[0:10])
        new_end = int(str(end_timestamp)[0:10])
        print(new_start / 2592000)
        print(new_end / 2592000)
        
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
                    print_sleep(self.interval)
                    time.sleep(self.interval)
                
            
                start_timestamp, end_timestamp, size = self._kernel(symbol, start_timestamp, end_timestamp)
                if start_timestamp > end_timestamp:
                    break
                if size == 0:
                    break
                # try:
                #     start_timestamp, end_timestamp, size = self._kernel(symbol, start_timestamp, end_timestamp)
                #     if size == 0:
                #         break
                # except:
                #     self.handle_network_issue(self._kernel, symbol)
                
                if test_flag:
                    break

                i = i + 1

        self.connector.close()

class FTXUSHistoryTradeDataCrawler(BaseExchangeHistoryCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(FTXUSHistoryTradeDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "ftxus")
        self.url = "https://ftx.us/api/markets/{}/trades?start_time={}&end_time={}"
        self.rate_limit = 29
        self.interval = 1

    def transform_symbol(self, symbol):
        pair = symbol.split("_")
        return pair[0] + "/" + pair[1]

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if res_data["success"] != True:
            return data_lst, start_timestamp, end_timestamp, 0

        for record in res_data["result"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["side"] == "buy" else False
            quoteQty = float(record["price"]) * float(record["size"])
            time = record["time"][0:19]
            d = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S")
            ts = int(d.replace(tzinfo=timezone.utc).timestamp())
            data += [record["id"], record["price"], record["size"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)

        size = len(res_data["result"])
        if size != 0:
            # find the smallest timestamp to be the new end_timestamp
            new_end_time = res_data["result"][-1]["time"][0:19]
            d = datetime.datetime.strptime(new_end_time, "%Y-%m-%dT%H:%M:%S")
            end_timestamp = int(d.replace(tzinfo=timezone.utc).timestamp())

        return data_lst, start_timestamp, end_timestamp, size

class FTXHistoryTradeDataCrawler(BaseExchangeHistoryCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(FTXHistoryTradeDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "ftx")
        self.url = "https://ftx.com/api/markets/{}/trades?start_time={}&end_time={}"
        self.rate_limit = 29
        self.interval = 1
    
    def transform_symbol(self, symbol):
        pair = symbol.split("_")
        return pair[0] + "/" + pair[1]

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if res_data["success"] != True:
            return data_lst, start_timestamp, end_timestamp, 0

        for record in res_data["result"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["side"] == "buy" else False
            quoteQty = float(record["price"]) * float(record["size"])
            time = record["time"][0:19]
            d = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S")
            ts = int(d.replace(tzinfo=timezone.utc).timestamp())
            data += [record["id"], record["price"], record["size"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)

        size = len(res_data["result"])
        if size != 0:
            # find the smallest timestamp to be the new end_timestamp
            new_end_time = res_data["result"][-1]["time"][0:19]
            d = datetime.datetime.strptime(new_end_time, "%Y-%m-%dT%H:%M:%S")
            end_timestamp = int(d.replace(tzinfo=timezone.utc).timestamp())

        return data_lst, start_timestamp, end_timestamp, size

class LiquidHistoryTradeDataCrawler(BaseExchangeHistoryCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(LiquidHistoryTradeDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "liquid")
        self.url = "https://api.liquid.com/executions?product_id={}&timestamp={}&limit=100"
        self.url_symbol_to_id = "https://api.liquid.com/products"
        self.rate_limit = 200
        self.interval = 300
    
    def transform_symbol(self, symbol):
        url = self.url_symbol_to_id
        response = requests.get(url)
        res_data = json.loads(response.text)
        product_id = -1

        pair = symbol.split("_")
        symbol = pair[0] + pair[1] # reconstruct from BTC_USDT to BTCUSDT
        
        for record in res_data:
            if record["currency_pair_code"] == symbol:
                product_id = record["id"]

        return product_id

    def construct_url(self, symbol, start_timestamp, end_timestamp):
        return self.url.format(symbol, start_timestamp, end_timestamp)

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if "message" in res_data and res_data["message"] == "Product not found":
            return data_lst, start_timestamp, end_timestamp, 0
        for record in res_data:
            data = [self.exch_name]
            isBuyerMaker = True if record["taker_side"] == "buy" else False
            quoteQty = float(record["price"]) * float(record["quantity"])
            data += [record["id"], record["price"], record["quantity"], quoteQty, record["created_at"], isBuyerMaker]
            data_lst.append(data)

        size = len(res_data)
        if size != 0:
            # find the smallest timestamp to be the new end_timestamp
            start_timestamp = res_data[-1]["created_at"] + 1

        return data_lst, start_timestamp, end_timestamp, size

class BinanceHistoryTradeDataCrawler(BaseExchangeHistoryCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(BinanceHistoryTradeDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "binance")
        self.url = "https://api1.binance.com/api/v3/klines?symbol={}&interval=1m&startTime={}&endTime={}"
        self.rate_limit = 29
        self.interval = 1

    def transform_symbol(self, symbol):
        pair = symbol.split("_")
        return pair[0] + pair[1]

    def construct_url(self, symbol, start_timestamp, end_timestamp):
        return self.url.format(symbol, start_timestamp, end_timestamp)

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if "message" in res_data:
            return data_lst, start_timestamp, end_timestamp, 0

        for record in res_data:
            data = [self.exch_name]
            isBuyerMaker = None
            price = (float(record[2]) + float(record[3])) / 2
            quantity = float(record[5])
            quoteQty = price * quantity
            opentime = int(record[0]) / 1000
            id = str(record[0])
            data += [id, price, quantity, quoteQty, opentime, isBuyerMaker]
            data_lst.append(data)

        size = len(res_data)
        if size != 0:
            # find the smallest timestamp to be the new end_timestamp
            start_timestamp = res_data[-1][6]

        return data_lst, start_timestamp, end_timestamp, size

class KrakenHistoryTradeDataCrawler(BaseExchangeHistoryCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(KrakenHistoryTradeDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "kraken")
        self.url = "https://api.kraken.com/0/public/Trades?pair={}&since={}"
        self.rate_limit = 6
        self.interval = 45
    
    def transform_symbol(self, symbol):
        pair = symbol.split("_")
        return pair[0] + pair[1]

    def construct_url(self, symbol, start_timestamp, end_timestamp):
        return self.url.format(symbol, start_timestamp, end_timestamp)

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if len(res_data["error"]) > 0:
            return data_lst, start_timestamp, end_timestamp, 0
        trade_lst, _ = res_data["result"].values()
        for record in trade_lst:
            data = [self.exch_name]
            price = float(record[0])
            qty = float(record[1])
            idx = 10000 * (float(record[0]) + float(record[2]))
            tick = int(record[2])
            quoteQty = price * qty
            isBuyerMaker = True if record[3] == "b" else False
            
            data += [idx, price, qty, quoteQty, tick, isBuyerMaker]
            data_lst.append(data)


        size = len(data_lst)
        if size != 0:
            # find the smallest timestamp to be the new end_timestamp
            start_timestamp = int(int(res_data["result"]["last"]) / 1000000000) + 1

        return data_lst, start_timestamp, end_timestamp, size

class BitfinexHistoryTradeDataCrawler(BaseExchangeHistoryCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(BitfinexHistoryTradeDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "bitfinex")
        self.url = "https://api-pub.bitfinex.com/v2/trades/{}/hist?limit=10000&start={}&end={}"
        self.rate_limit = 90
        self.interval = 60
    
    def transform_symbol(self, symbol):
        pair = symbol.split("_")
        return "t" + pair[0] + "USD"

    def construct_url(self, symbol, start_timestamp, end_timestamp):
        return self.url.format(symbol, start_timestamp * 1000, end_timestamp * 1000)

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if len(res_data) == 0:
            return data_lst, start_timestamp, end_timestamp, 0
        for record in res_data:
            data = [self.exch_name]
            try:
                tick = int(record[1]) / 1000
            except:
                continue
            qty = abs(record[2])
            isBuyerMaker = True if record[2] > 0 else False
            quoteQty = qty * record[3]
            data += [record[0], record[3], qty, quoteQty, tick, isBuyerMaker]
            data_lst.append(data)

        size = len(data_lst)
        if size != 0:
            # find the smallest timestamp to be the new end_timestamp
            end_timestamp = int(res_data[-1][1] / 1000) - 1

        return data_lst, start_timestamp, end_timestamp, size

class LBankHistoryTradeDataCrawler(BaseExchangeHistoryCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(LBankHistoryTradeDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "lbank")
        self.url = "https://api.lbkex.com/v2/trades.do?symbol={}&size=600&time={}"
        self.rate_limit = 200
        self.interval = 10
    
    def transform_symbol(self, symbol):
        sym = symbol.split("_")
        sym = [k.lower() for k in sym]
        return "_".join(sym)

    def construct_url(self, symbol, start_timestamp, end_timestamp):
        return self.url.format(symbol, start_timestamp * 1000, end_timestamp * 1000)

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if res_data["result"] == "false":
            return data_lst, start_timestamp, end_timestamp, 0
        
        for record in res_data["data"]:
            data = [self.exch_name]
            quoteQty = float(record["price"]) * float(record["amount"])
            tick = int(record["date_ms"] / 1000)
            isBuyerMaker = True if record["type"] == "buy" else False
            data += [record["tid"], record["price"], record["amount"], quoteQty, tick, isBuyerMaker]
            data_lst.append(data)
        

        size = len(data_lst)
        if size != 0:
            start_timestamp = int(data_lst[-1][5]) + 1

        return data_lst, start_timestamp, end_timestamp, size