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
    
    def transform_symbol(self, symbol):
        return symbol
    
    def construct_url(self, symbol):
        return self.url.format(symbol)
    
    def request_data(self, url):
        response = requests.get(url)
        return json.loads(response.text)

    def parse_data(self, data):
        raise NotImplementedError
    
    def write_into_db(self, data_lst, symbol):
        self.connector.insert_trade_data(data_lst, symbol)

    def _kernel(self, symbol):
        q_symbol = self.transform_symbol(symbol)
        url = self.construct_url(q_symbol)
        data = self.request_data(url)
        data_lst = self.parse_data(data)
        self.write_into_db(data_lst, symbol)

    def run(self, test_flag=False):
        """Kernel function"""
        while True:
            print_write_data()
            for symbol in self.symbols:
                try:
                    self._kernel(symbol)
                except:
                    self.handle_network_issue(self._kernel, symbol)
            print_sleep(self.interval)
            if test_flag:
                break
            time.sleep(self.interval)
        self.connector.close()


class BinanceTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BinanceTradeDataCrawler, self).__init__(db_name, interval, symbols, "binance")
        self.url = "https://api1.binance.com/api/v3/trades?symbol={}&limit=1000"

    def parse_data(self, res_data):
        data_lst = []
        for item in res_data:
            data = [self.exch_name]
            data += [item["id"], item["price"], item["qty"], item["quoteQty"], item["time"], item["isBuyerMaker"]]
            data[5] = int(data[5] / 1000)
            data_lst.append(tuple(data))
        return data_lst


class HuobiTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(HuobiTradeDataCrawler, self).__init__(db_name, interval, symbols, "huobi")
        self.url = "https://api.hbdm.com/linear-swap-ex/market/history/trade?contract_code={}&size=1000"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def parse_data(self, res_data):
        data_lst = []
        for item in res_data["data"]:
            for record in item["data"]:
                data = [self.exch_name]
                isBuyerMaker = True if record["direction"] == "buy" else False
                data += [record["id"], record["price"], record["quantity"], record["trade_turnover"],  record["ts"], isBuyerMaker]
                data[5] = int(data[5] / 1000)
                data_lst.append(data)
        return data_lst


class CoinbaseTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(CoinbaseTradeDataCrawler, self).__init__(db_name, interval, symbols, "coinbase")
        self.url = "https://api.pro.coinbase.com/products/{}/trades"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def parse_data(self, res_data):
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
                rtime = record["time"].replace("Z", "")
                tick = date2tick(rtime.split(".")[0].replace("T", " "))
                data += [record["trade_id"], record["price"], record["size"], quoteQty, tick, isBuyerMaker]
                data_lst.append(data)

        return data_lst

class HuobiKoreaTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(HuobiKoreaTradeDataCrawler, self).__init__(db_name, interval, symbols, "huobikorea")
        self.url = "https://api-cloud.huobi.co.kr/market/history/trade?symbol={}&size=500"
    
    def transform_symbol(self, symbol):
        return symbol.lower()
    
    def parse_data(self, res_data):
        data_lst = []
        for item in res_data["data"]:
            for record in item["data"]:
                data = [self.exch_name]
                isBuyerMaker = True if record["direction"] == "buy" else False
                quoteQty = record["price"] * record["amount"]
                data += [record["trade-id"], record["price"], record["amount"], quoteQty,  record["ts"], isBuyerMaker]
                data[5] = int(data[5] / 1000)
                data_lst.append(data)
        return data_lst

class ProBitGlobalTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(ProBitGlobalTradeDataCrawler, self).__init__(db_name, interval, symbols, "probitglobal")
        self.url = "https://api.probit.com/api/exchange/v1/trade?market_id={}&start_time={}&end_time={}&limit=1000"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    

    def construct_url(self, symbol):
        utc_tz = pytz.timezone('UTC')
        starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1) - datetime.timedelta(seconds=5)
        starttime = starttime.isoformat()
        start = str(starttime)
        start = start[0:23]
        start = start + 'Z'

        endtime = datetime.datetime.now(tz = utc_tz)
        endtime = endtime.isoformat()
        end = str(endtime)
        end = end[0:23]
        end = end + 'Z'
        return self.url.format(symbol, start, end)
    
    def parse_data(self, res_data):
        data_lst = []
        for record in res_data["data"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["side"] == "buy" else False
            quoteQty = float(record["price"]) * float(record["quantity"])
            d = datetime.datetime.strptime(record["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
            ts = int(d.timestamp())
            data += [record["id"], record["price"], record["quantity"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)
        return data_lst

class FTXUSTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(FTXUSTradeDataCrawler, self).__init__(db_name, interval, symbols, "ftxus")
        self.url = "https://ftx.us/api/markets/{}/trades?start_time={}&end_time={}"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "/" + "USD"
    
    def construct_url(self, symbol):
        utc_tz = pytz.timezone('UTC')
        starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1) - datetime.timedelta(seconds=5)
        start = int(starttime.timestamp())

        endtime = datetime.datetime.now(tz = utc_tz)
        end = int(endtime.timestamp())

        return self.url.format(symbol, start, end)

    def parse_data(self, res_data):
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
        return data_lst

class BittrexTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BittrexTradeDataCrawler, self).__init__(db_name, interval, symbols, "bittrex")
        self.url = "https://api.bittrex.com/v3/markets/{}/trades"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def parse_data(self, res_data):
        if isinstance(res_data, dict):
            if res_data["code"] == "MARKET_DOES_NOT_EXIST": return
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
        return data_lst

class OKExTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(OKExTradeDataCrawler, self).__init__(db_name, interval, symbols, "okex")
        self.url = "https://www.okex.com/api/spot/v3/instruments/{}/trades?limit=100"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def parse_data(self, res_data):
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
        return data_lst

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
    
    def construct_url(self, symbol):
        utc_tz = pytz.timezone('UTC')
        starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1) - datetime.timedelta(seconds=5)
        ts = int(starttime.timestamp())
        return self.url.format(symbol, ts)
    
    def parse_data(self, res_data):
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            isBuyerMaker = True if record["taker_side"] == "buy" else False
            quoteQty = float(record["price"]) * float(record["quantity"])
            data += [record["id"], record["price"], record["quantity"], quoteQty, record["created_at"], isBuyerMaker]
            data_lst.append(data)
        return data_lst

class CryptoComExchangeTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(CryptoComExchangeTradeDataCrawler, self).__init__(db_name, interval, symbols, "cryptocomexchange")
        self.url = "https://api.crypto.com/v2/public/get-trades?instrument_name={}"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "_" + "USDT"
    
    def parse_data(self, res_data, symbol):
        data_lst = []
        for record in res_data["result"]["data"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["s"] == "BUY" else False
            quoteQty = float(record["p"]) * float(record["q"])
            data += [record["d"], record["p"], record["q"], quoteQty,  record["t"], isBuyerMaker]
            data[5] = int(int(data[5]) / 1000)
            data_lst.append(data)
        return data_lst

class AscendEXTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(AscendEXTradeDataCrawler, self).__init__(db_name, interval, symbols, "ascendex")
        self.url = "https://ascendex.com/api/pro/v1/trades?symbol={}&n=50"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "/" + "USDT"
    
    def parse_data(self, res_data):
        data_lst = []
        for record in res_data["data"]["data"]:
            data = [self.exch_name]
            trade_id = str(record["seqnum"])
            quoteQty = float(record["p"]) * float(record["q"])
            data += [trade_id, record["p"], record["q"], quoteQty,  record["ts"], record["bm"]]
            data[5] = int(int(data[5]) / 1000)
            data_lst.append(data)

        return data_lst

class CoinDCXTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(CoinDCXTradeDataCrawler, self).__init__(db_name, interval, symbols, "coindcx")
        self.url = "https://public.coindcx.com/market_data/trade_history?pair={}&limit=50"
    
    def transform_symbol(self, symbol):
        return "B-" + symbol[:-4] + "_" + "USDT"
    
    def request_data(self, url):
        response = requests.get(url)
        if response.text == "":
            return ""
        return json.loads(response.text)
    
    def parse_data(self, res_data):
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            trade_id = symbol + str(record["T"]) + str(record["q"])
            quoteQty = float(record["p"]) * float(record["q"])
            data += [trade_id, record["p"], record["q"], quoteQty,  record["T"], record["m"]]
            data[5] = int(int(data[5]) / 1000)
            data_lst.append(data)
        return data_lst


class KrakenTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(KrakenTradeDataCrawler, self).__init__(db_name, interval, symbols, "kraken")
        self.url = "https://api.kraken.com/0/public/Trades?pair={}&since={}"

    def construct_url(self, symbol):
        end_time = time.time()
        start_time = int(end_time - 60 * 1.5)
        return self.url.format(symbol, start_time)

    def parse_data(self, res_data):
        if len(res_data["error"]) > 0:
            return
        data_lst = []
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
        return data_lst


class FTXTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(FTXTradeDataCrawler, self).__init__(db_name, interval, symbols, "ftx")
        self.url = "https://ftx.com/api/markets/{}/trades?start_time={}&end_time={}"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "/" + "USDT"
    
    # def request_data(self, symbol):
    def construct_url(self, symbol):

        end_time = int(time.time())
        start_time = int(end_time - 60 * 1.5)
        
        return self.url.format(symbol, start_time, end_time)
    
    def parse_data(self, res_data):
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
        
        return data_lst

    
class KucoinTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(KucoinTradeDataCrawler, self).__init__(db_name, interval, symbols, "kucoin")
        self.url = "https://api.kucoin.com/api/v1/market/histories?symbol={}"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "-" + "USDT"
    
    def parse_data(self, res_data):
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
        
        return data_lst


class BitfinexTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BitfinexTradeDataCrawler, self).__init__(db_name, interval, symbols, "bitfinex")
        self.url = "https://api-pub.bitfinex.com/v2/trades/{}/hist?limit=1000&start={}&end={}"
    
    def transform_symbol(self, symbol):
        return "t" + symbol[:-1]
    

    def construct_url(self, symbol):
        end_time = time.time()
        start_time = end_time - 60 * 1.5
        start_time = int(1000 * start_time)
        end_time = int(1000 * end_time)
        return self.url.format(symbol, start_time, end_time)
    
    def parse_data(self, res_data):
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
        return data_lst

    
class GateIOTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(GateIOTradeDataCrawler, self).__init__(db_name, interval, symbols, "gate.io")
        self.url = "https://api.gateio.ws/api/v4/spot/trades?currency_pair={}&limit=1000"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "_" + "USDT"
    
    def parse_data(self, res_data):
        if isinstance(res_data, list) and len(res_data) == 0 or isinstance(res_data, dict):
            return 
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            quoteQty = float(record["amount"]) * float(record["price"])
            isBuyerMaker = True if record["side"] == "buy" else False
            data += [record["id"], record["price"], record["amount"], quoteQty, record["create_time"], isBuyerMaker]
            data_lst.append(data)
        return data_lst

class BinanceUSTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BinanceUSTradeDataCrawler, self).__init__(db_name, interval, symbols, "binance.us")
        self.url = "https://api.binance.us/api/v3/trades?symbol={}&limit=1000"

    def parse_data(self, res_data):
        if isinstance(res_data, dict) and res_data["code"] == -1121:
            return
        data_lst = []
        for item in res_data:
            data = [self.exch_name]
            data += [item["id"], item["price"], item["qty"], item["quoteQty"], item["time"], item["isBuyerMaker"]]
            data[5] = int(data[5] / 1000)
            data_lst.append(tuple(data))
        return data_lst


class BitstampTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BitstampTradeDataCrawler, self).__init__(db_name, interval, symbols, "bitstamp")
        self.url = "https://www.bitstamp.net/api/v2/transactions/{}/?time=minute"
    
    def transform_symbol(self, symbol):
        return symbol.lower()
    
    def request_data(self, url):
        response = requests.get(url)
        r_str = response.text
        if "script" in r_str: 
            return r_str
        return json.loads(response.text)
    
    def parse_data(self, res_data):
        if len(res_data) == 0 or isinstance(res_data, str):
            return
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            quoteQty = float(record["amount"]) * float(record["price"])
            isBuyerMaker = True if record["type"] == "0" else False
            data += [record["tid"], record["price"], record["amount"], quoteQty, record["date"], isBuyerMaker]
            data_lst.append(data)
        return data_lst


class GeminiTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(GeminiTradeDataCrawler, self).__init__(db_name, interval, symbols, "gemini")
        self.url = "https://api.gemini.com/v1/trades/{}?timestamp={}&limit_trades=1000"
    
    def transform_symbol(self, symbol):
        return symbol[:-1]
    
    def construct_url(self, symbol):
        now = time.time()
        since = int(now - 60 * 1.5)
        return self.url.format(symbol, since)
    
    def parse_data(self, res_data):
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
        return data_lst
    

class BitflyerTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BitflyerTradeDataCrawler, self).__init__(db_name, interval, symbols, "bitflyer")
        self.url = "https://api.bitflyer.com/v1/getexecutions?product_code={}&count=100"
    
    def transform_symbol(self, symbol):
        return symbol[:-4] + "_" + "USD"
    
    def parse_data(self, res_data):
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
        return data_lst


class PoloniexTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(PoloniexTradeDataCrawler, self).__init__(db_name, interval, symbols, "poloniex")
        self.url = "https://poloniex.com/public?command=returnTradeHistory&currencyPair={}&start={}&end={}"
    
    def transform_symbol(self, symbol):
        return symbol[-4:] + "_" + symbol[:-4]
    
    def construct_url(self, symbol):
        end_time = time.time()
        start_time = end_time - 60 * 1.5
        end_time = int(end_time)
        start_time = int(start_time)
        return self.url.format(symbol, start_time, end_time)
    
    def parse_data(self, res_data):
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
        return data_lst

    
class BitexenTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BitexenTradeDataCrawler, self).__init__(db_name, interval, symbols, "bitexen")
        self.url = "https://www.bitexen.com/api/v1/order_book/{}/"
    
    def parse_data(self, res_data):
        if res_data["data"] == None:
            return
        data_lst = []
        for record in res_data["data"]["last_transactions"]:
            # {"amount": "0.09647533", "price": "32409.64", "time": "1625792177.446199", "type": "B"}\
            data = [self.exch_name]
            tick = record["time"].split(".")[0]
            tid = "".join(record["time"].split("."))
            quoteQty = float(record["amount"]) * float(record["price"])
            isBuyerMaker = True if record["type"] == "B" else False
            data += [tid, record["price"], record["amount"], quoteQty, tick, isBuyerMaker]
            data_lst.append(data)
        return data_lst
