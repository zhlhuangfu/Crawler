"""
It contains the class Crawler for crawling the price info,
CoinMarketCap API is used.
Along with the Crawler, there are other methods relate to use the API
"""
import pdb
import json
import time
import datetime

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


class KrakenTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(KrakenTradeDataCrawler, self).__init__(db_name, interval, symbols, "kraken")
        self.url = "https://api.kraken.com/0/public/Trades?pair={}&since={}"
    
    def transform_symbol(self, symbol):
        return symbol

    def request_data(self, symbol):
        end_time = time.time()
        start_time = int(end_time - 60 * 1.5)
        url = self.url.format(symbol, start_time)
        response = requests.get(url)
        return json.loads(response.text)

    def write_into_db(self, res_data, symbol):
        if len(res_data["error"]) > 0:
            return
        
        data_lst = []
        q_symbol = "XBTUSDT" if symbol == "BTCUSDT" else symbol
        trade_lst = res_data["result"][q_symbol]
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
        self.connector.insert_trade_data(data_lst, symbol)


class FTXTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(FTXTradeDataCrawler, self).__init__(db_name, interval, symbols, "ftx")
        self.url = "https://ftx.com/api/markets/{}/trades?start_time={}&end_time={}"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "/" + "USDT"
    
    def request_data(self, symbol):

        end_time = int(time.time())
        start_time = int(end_time - 60 * 1.5)
        
        url = self.url.format(symbol, start_time, end_time)
        response = requests.get(url)

        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        if not res_data["success"]:
            return
        data_lst = []
        date2tick = lambda time_str: time.mktime(datetime.datetime.strptime(time_str,
                                       r"%Y-%m-%d %H:%M:%S").timetuple())
        
        for record in res_data["result"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["side"] == "buy" else False
            quoteQty = float(record["size"]) * float(record["price"])
            tick = date2tick(record["time"].split(".")[0].replace("T", " "))
            data += [record["id"], record["price"], record["size"], quoteQty, tick, isBuyerMaker]
            data_lst.append(data)
        
        self.connector.insert_trade_data(data_lst, symbol)

    
class KucoinTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(KucoinTradeDataCrawler, self).__init__(db_name, interval, symbols, "kucoin")
        self.url = "https://api.kucoin.com/api/v1/market/histories?symbol={}"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def request_data(self, symbol):
        url = self.url.format(symbol)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        if len(res_data["data"]) == 0:
            return
        data_lst = []

        for record in res_data["data"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["side"] == "buy" else False
            quoteQty = float(record["size"]) * float(record["price"])
            tick = str(record["time"])[:10]
            data += [record["sequence"], record["price"], record["size"], quoteQty, tick, isBuyerMaker]
            data_lst.append(data)
        
        self.connector.insert_trade_data(data_lst, symbol)


class BitfinexTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BitfinexTradeDataCrawler, self).__init__(db_name, interval, symbols, "bitfinex")
        self.url = "https://api-pub.bitfinex.com/v2/trades/{}/hist?limit=1000&start={}&end={}"
    
    def transform_symbol(self, symbol):
        return "t" + symbol[:-1]
    
    def request_data(self, symbol):
        end_time = time.time()
        start_time = end_time - 60 * 1.5
        start_time = int(1000 * start_time)
        end_time = int(1000 * end_time)
        pdb.set_trace()
        url = self.url.format(symbol, start_time, end_time)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        if len(res_data) == 0:
            return
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            tick = record[1] // 1000
            qty = abs(record[2])
            isBuyerMaker = True if record[2] > 0 else False
            quoteQty = qty * record[3]
            data += [record[0], record[3], qty, quoteQty, tick, isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)

    
class GateIOTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(GateIOTradeDataCrawler, self).__init__(db_name, interval, symbols, "gate.io")
        self.url = "https://api.gateio.ws/api/v4/spot/trades?currency_pair={}&limit=1000"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "_" + "USDT"
    
    def request_data(self, symbol):
        url = self.url.format(symbol)

        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        if isinstance(res_data, list) and len(res_data) == 0 or isinstance(res_data, dict):
            return 
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            quoteQty = float(record["amount"]) * float(record["price"])
            isBuyerMaker = True if record["side"] == "buy" else False
            data += [record["id"], record["price"], record["amount"], quoteQty, record["create_time"], isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)

class BinanceUSTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BinanceUSTradeDataCrawler, self).__init__(db_name, interval, symbols, "binance.us")
        self.url = "https://api.binance.us/api/v3/trades?symbol={}&limit=1000"
    
    def transform_symbol(self, symbol):
        return symbol

    def request_data(self, symbol):
        url = self.url.format(symbol)
        response = requests.get(url)
        return json.loads(response.text)

    def write_into_db(self, res_data, symbol):
        if isinstance(res_data, dict) and res_data["code"] == -1121:
            return
        data_lst = []
        for item in res_data:
            data = [self.exch_name]
            data += [item["id"], item["price"], item["qty"], item["quoteQty"], item["time"], item["isBuyerMaker"]]
            data[5] = int(data[5] / 1000)
            data_lst.append(tuple(data))
        self.connector.insert_trade_data(data_lst, symbol)


class BitstampTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BitstampTradeDataCrawler, self).__init__(db_name, interval, symbols, "bitstamp")
        self.url = "https://www.bitstamp.net/api/v2/transactions/{}/?time=minute"
    
    def transform_symbol(self, symbol):
        return symbol.lower()
    
    def request_data(self, symbol):
        url = self.url.format(symbol)
        response = requests.get(url)
        r_str = response.text
        if "script" in r_str: 
            return r_str
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        if len(res_data) == 0 or isinstance(res_data, str):
            return
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            quoteQty = float(record["amount"]) * float(record["price"])
            isBuyerMaker = True if record["type"] == "0" else False
            data += [record["tid"], record["price"], record["amount"], quoteQty, record["date"], isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)


class GeminiTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(GeminiTradeDataCrawler, self).__init__(db_name, interval, symbols, "gemini")
        self.url = "https://api.gemini.com/v1/trades/{}?timestamp={}&limit_trades=1000"
    
    def transform_symbol(self, symbol):
        return symbol[:-1]
    
    def request_data(self, symbol):
        now = time.time()
        since = int(now - 60 * 1.5)
        url = self.url.format(symbol, since)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        # {"timestamp":1625599445,"timestampms":1625599445157,"tid":46900544038,"price":"33931.81","amount":"0.00146621","exchange":"gemini","type":"buy"}
        if isinstance(res_data, dict) and res_data["result"] == "error":
            return
        data_lst = []
        for record in res_data:

            data = [self.exch_name]
            quoteQty = float(record["amount"]) * float(record["price"])
            isBuyerMaker = True if record["type"] == "buy" else False
            data += [record["tid"], record["price"], record["amount"], quoteQty, record["timestamp"], isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)
    

class BitflyerTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BitflyerTradeDataCrawler, self).__init__(db_name, interval, symbols, "bitflyer")
        self.url = "https://api.bitflyer.com/v1/getexecutions?product_code={}&count=100"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "_" + "USD"
    
    def request_data(self, symbol):
        url = self.url.format(symbol)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        # {"id":2235850539,"side":"BUY","price":34108.93,"size":0.12156082,"exec_date":"2021-07-06T23:44:32.76","buy_child_order_acceptance_id":"JRF20210706-234432-301999","sell_child_order_acceptance_id":"JRF20210706-234422-134754"}
        if len(res_data) == 0:
            return
        date2tick = lambda time_str: time.mktime(datetime.datetime.strptime(time_str,
                                       r"%Y-%m-%d %H:%M:%S").timetuple())
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            tick = date2tick(record["exec_date"].split(".")[0].replace("T", " "))
            quoteQty = float(record["price"]) * float(record["size"])
            isBuyerMaker = True if record["side"] == "BUY" else False
            data += [record["id"], record["price"], record["size"], quoteQty, tick, isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)


class PoloniexTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(PoloniexTradeDataCrawler, self).__init__(db_name, interval, symbols, "poloniex")
        self.url = "https://poloniex.com/public?command=returnTradeHistory&currencyPair={}&start={}&end={}"
    
    def transform_symbol(self, symbol):
        return symbol[-4:] + "_" + symbol[:-4]
    
    def request_data(self, symbol):
        end_time = time.time()
        start_time = end_time - 60 * 1.5
        end_time = int(end_time)
        start_time = int(start_time)
        url = self.url.format(symbol, start_time, end_time)
        response = requests.get(url)
        return json.loads(response.text)
    
    def write_into_db(self, res_data, symbol):
        if isinstance(res_data, dict) and "error" in res_data.keys():
            return
        # {"globalTradeID":508982120,"tradeID":40158513,"date":"2021-01-13 00:56:11","type":"buy","rate":"32634.47060135","amount":"0.01700323","total":"554.89140956","orderNumber":840128031121}
        date2tick = lambda time_str: time.mktime(datetime.datetime.strptime(time_str,
                                       r"%Y-%m-%d %H:%M:%S").timetuple())
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            isBuyerMaker = True if record["type"] == "buy" else False
            tick = date2tick(record["date"])
            data += [record["tradeID"], record["rate"], record["amount"], record["total"], tick, isBuyerMaker]
            data_lst.append(data)
        self.connector.insert_trade_data(data_lst, symbol)