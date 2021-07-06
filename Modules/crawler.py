"""
It contains the class Crawler for crawling the price info,
CoinMarketCap API is used.
Along with the Crawler, there are other methods relate to use the API
"""
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
            self.process()
            # try:
            #     self.process()
            # except:
            #     self.handle_network_issue()
            print_sleep(self.interval)
            time.sleep(self.interval)
        self.connector.close()


class BaseExchangeCrawler(BaseCrawler):
    def __init__(self, db_name, interval, symbols, exch_name):
        super(BaseExchangeCrawler, self).__init__(db_name, interval)
        self.exch_name = exch_name
        self.connector.create_meta_table()

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
        for symbol in self.symbols:
            q_symbol = self.transform_symbol(symbol)
            data = self.request_data(q_symbol)
            self.write_into_db(data, symbol)
        # try:
        #     for symbol in self.symbols:
        #         q_symbol = self.transform_symbol(symbol)
        #         data = self.request_data(q_symbol)
        #         self.write_into_db(data, symbol)
        # except:
        #     raise ConnectionError


class BinanceTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BinanceTradeDataCrawler, self).__init__(db_name, interval, symbols, "binance")
        self.url = "https://api1.binance.com/api/v3/trades?symbol={}&limit=1000"

    def transform_symbol(self, symbol):
        return symbol

    def request_data(self, symbol):
        url = self.url.format(symbol)
        response = requests.get(url)
        return json.loads(response.text)

    def write_into_db(self, res_data, symbol):
        data_lst = []
        for item in res_data:
            data = [self.exch_name]
            data += [item["id"], item["price"], item["qty"], item["quoteQty"], item["time"], item["isBuyerMaker"]]
            data[5] = int(data[5] / 1000)
            data_lst.append(tuple(data))
        self.connector.insert_trade_data(data_lst, symbol)


class HuobiTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(HuobiTradeDataCrawler, self).__init__(db_name, interval, symbols, "huobi")
        self.url = "https://api.hbdm.com/linear-swap-ex/market/history/trade?contract_code={}&size=1000"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def request_data(self, symbol):
        url = self.url.format(symbol)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        data_lst = []
        for item in res_data["data"]:
            for record in item["data"]:
                data = [self.exch_name]
                isBuyerMaker = True if record["direction"] == "buy" else False
                data += [record["id"], record["price"], record["quantity"], record["trade_turnover"],  record["ts"], isBuyerMaker]
                data[5] = int(data[5] / 1000)
                data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)


class CoinbaseTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(CoinbaseTradeDataCrawler, self).__init__(db_name, interval, symbols, "coinbase")
        self.url = "https://api.pro.coinbase.com/products/{}/trades"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def request_data(self, symbol):
        url = self.url.format(symbol)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        if isinstance(res_data, dict) and "message" in res_data.keys():
            if res_data["message"] == "NotFound":
                return
        data_lst = []
        date2tick = lambda time_str: time.mktime(datetime.datetime.strptime(time_str,
                                       r"%Y-%m-%d %H:%M:%S").timetuple())
        for record in res_data:
            data = [self.exch_name]
            isBuyerMaker = True if record["side"] == "buy" else False
            quoteQty = float(record["size"]) * float(record["price"])
            tick = date2tick(record["time"].split(".")[0].replace("T", " "))
            data += [record["trade_id"], record["price"], record["size"], quoteQty, tick, isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)

class HuobiKoreaTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(HuobiKoreaTradeDataCrawler, self).__init__(db_name, interval, symbols, "huobikorea")
        self.url = "https://api-cloud.huobi.co.kr/market/history/trade?symbol={}&size=500"
    
    def transform_symbol(self, symbol):
        return symbol.lower()
    
    def request_data(self, symbol):
        url = self.url.format(symbol)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        data_lst = []
        for item in res_data["data"]:
            for record in item["data"]:
                data = [self.exch_name]
                isBuyerMaker = True if record["direction"] == "buy" else False
                quoteQty = record["price"] * record["amount"]
                data += [record["trade-id"], record["price"], record["amount"], quoteQty,  record["ts"], isBuyerMaker]
                data[5] = int(data[5] / 1000)
                data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)

class ProBitGlobalTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(ProBitGlobalTradeDataCrawler, self).__init__(db_name, interval, symbols, "probitglobal")
        self.url = "https://api.probit.com/api/exchange/v1/trade?market_id={}&start_time={}&end_time={}&limit=1000"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def request_data(self, symbol):
        utc_tz = pytz.timezone('UTC')
        starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1)
        starttime = starttime.isoformat()
        start = str(starttime)
        start = start[0:23]
        start = start + 'Z'

        endtime = datetime.datetime.now(tz = utc_tz)
        endtime = endtime.isoformat()
        end = str(endtime)
        end = end[0:23]
        end = end + 'Z'
        url = self.url.format(symbol, start, end)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        data_lst = []
        for record in res_data["data"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["side"] == "buy" else False
            quoteQty = float(record["price"]) * float(record["quantity"])
            d = datetime.datetime.strptime(record["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
            ts = int(d.timestamp())
            data += [record["id"], record["price"], record["quantity"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)

class FTXUSTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(FTXUSTradeDataCrawler, self).__init__(db_name, interval, symbols, "ftxus")
        self.url = "https://ftx.us/api/markets/{}/trades?start_time={}&end_time={}"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "/" + "USD"
    
    def request_data(self, symbol):
        utc_tz = pytz.timezone('UTC')
        starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1)
        start = int(starttime.timestamp())

        endtime = datetime.datetime.now(tz = utc_tz)
        end = int(endtime.timestamp())

        url = self.url.format(symbol, start, end)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        data_lst = []
        for record in res_data["result"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["side"] == "buy" else False
            quoteQty = record["price"] * record["size"]
            time = record["time"][0:23]
            d = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f")
            ts = int(d.timestamp())
            data += [record["id"], record["price"], record["size"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)

class BittrexTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BittrexTradeDataCrawler, self).__init__(db_name, interval, symbols, "bittrex")
        self.url = "https://api.bittrex.com/v3/markets/{}/trades"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def request_data(self, symbol):
        url = self.url.format(symbol)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            isBuyerMaker = True if record["takerSide"] == "BUY" else False
            quoteQty = float(record["rate"]) * float(record["quantity"])
            time = record["executedAt"][0:19]
            d = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S")
            ts = int(d.timestamp())
            data += [record["id"], record["rate"], record["quantity"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)

class OKExTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(OKExTradeDataCrawler, self).__init__(db_name, interval, symbols, "okex")
        self.url = "https://www.okex.com/api/spot/v3/instruments/{}/trades?limit=100"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def request_data(self, symbol):
        url = self.url.format(symbol)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            isBuyerMaker = True if record["side"] == "buy" else False
            quoteQty = float(record["price"]) * float(record["size"])
            time = record["timestamp"][0:23]
            d = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f")
            ts = int(d.timestamp())
            data += [record["trade_id"], record["price"], record["size"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)

class LiquidTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(LiquidTradeDataCrawler, self).__init__(db_name, interval, symbols, "liquid")
        self.url = "https://api.liquid.com/executions?product_id={}&timestamp={}&limit=100"
        self.url_symbol_to_id = "https://api.liquid.com/products"
    
    def transform_symbol(self, symbol):
        url = self.url_symbol_to_id
        response = requests.get(url)
        res_data = json.loads(response.text)
        product_id = -1
        for record in res_data:
            if record["currency_pair_code"] == symbol:
                product_id = record["id"]

        if product_id == -1:
            print(product_id)
        return product_id
    
    def request_data(self, symbol):
        utc_tz = pytz.timezone('UTC')
        starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1)
        ts = int(starttime.timestamp())

        url = self.url.format(symbol, ts)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            isBuyerMaker = True if record["taker_side"] == "buy" else False
            quoteQty = float(record["price"]) * float(record["quantity"])
            data += [record["id"], record["price"], record["quantity"], quoteQty, record["created_at"], isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)