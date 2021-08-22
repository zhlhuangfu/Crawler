"""
It contains the class Crawler for crawling the price info,
CoinMarketCap API is used.
Along with the Crawler, there are other methods relate to use the API
"""
import pdb
import json
import time
import pytz
import datetime

from tqdm import tqdm

import requests
from .mysql_connector import CryptoCoinConnector
from .print_section import print_error_sleep, print_sleep, print_write_data, print_error_pair


class BaseCrawler():
    """Base abstract of Crawler"""

    def __init__(self, db_name, interval):
        self.interval = interval
        self.load_top_k_pair(500)
        db_names = []
        for i in range(49):
            lower = i * 10 + 1
            upper = lower + 9
            db_name = "realtime_transactions_top{}_{}coins"
            db_name = db_name.format(str(lower), str(upper))
            db_names.append(db_name)
        self.connector = CryptoCoinConnector(db_names)
        
    def write_into_db(self, json_data):
        """Write infomation into DB"""
        raise NotImplementedError

    def request_data(self, *kwargs):
        raise NotImplementedError

    def _kernel(self, *kwargs):
        raise NotImplementedError

    def load_top_k_pair(self, k):
        with open("sorted_top_500_symbols.json") as fi:
            dct = json.load(fi)
        self.db_name_dict = {}
        for t in range(k):
            lower = int(t / 10) * 10 + 1
            upper = lower + 9
            db_name = "realtime_transactions_top{}_{}coins"
            db_name = db_name.format(str(lower), str(upper))
            symbol = dct[str(t + 1)] + "_USDT"
            self.db_name_dict[symbol] = db_name

    def handle_network_issue(self, Exp, function, symbol):
        """Handle network issue"""
        FLAG = False
        for i in range(1, 60 * 10 + 1):
            if FLAG : break
            print_error_pair(Exp, self.exch_name + " " + symbol, i)
            time.sleep(i)
            try:
                function(symbol)
                FLAG = True

            except Exception as e:
                Exp = e
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
        self.symbols = symbols
        self.verbose = True
        self.use_proxy = False
    
    def set_verbose(self, flag):
        self.verbose = flag

    def transform_symbol(self, symbol):
        return symbol
    
    def construct_url(self, symbol):
        return self.url.format(symbol)
    
    def request_data(self, url):
        data, response, proxies = None, None, None
        try:
            if self.use_proxy:
                proxies = {"http": "http://5.79.73.131:13150",
                        "https": "http://5.79.73.131:13150"}
            response = requests.get(url, proxies=proxies)
            data = json.loads(response.text)

            return data
        except Exception as e:
            if response != None:
                return data
            else:
                raise e

    def parse_data(self, data, *kwargs):
        raise NotImplementedError

    def write_into_db(self, data_lst, symbol):
        if data_lst == None or len(data_lst) == 0:
            return
        db_name = self.db_name_dict[symbol]
        self.connector.use_database(db_name)

        # divide the data_lst into data_group with same table_index, and insert them into database
        while len(data_lst) != 0:
            table_index = int(int(data_lst[0][5]) / 2592000)

            data_group = []
            data_group.append(data_lst[0])
            del data_lst[0]

            j = 0
            for i in range(len(data_lst)):
                index = int(int(data_lst[j][5]) / 2592000)
                if table_index == index:
                    data_group.append(data_lst[j])
                    del data_lst[j]
                else:
                    j = j + 1

            self.connector.create_trade_info_table(symbol, table_index, table_index)
            self.connector.insert_trade_data(data_group, symbol, table_index)

    def _kernel(self, symbol):
        try:
            q_symbol = self.transform_symbol(symbol)
            url = self.construct_url(q_symbol)
            data = self.request_data(url)
            if data == None:
                return
            data_lst = self.parse_data(data)
            tab_sym = "_".join(symbol.split("-"))
            self.write_into_db(data_lst, tab_sym)
        except Exception as e:
            raise e

    def run(self, test_flag=False):
        """Kernel function"""
        while True:
            if self.verbose:
                print_write_data()
            for symbol in self.symbols:
                try:
                    self._kernel(symbol)
                except Exception as e:
                    self.handle_network_issue(e, self._kernel, symbol)
            
            if test_flag:
                break
            print_sleep(self.interval)
            time.sleep(self.interval)
        self.connector.close()


class BinanceTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BinanceTradeDataCrawler, self).__init__(db_name, interval, symbols, "binance")
        self.url = "https://api1.binance.com/api/v3/trades?symbol={}&limit=1000"

    def transform_symbol(self, symbol):
        sym = symbol.split("-")
        sym = "".join(sym)
        return sym

    def parse_data(self, res_data):
        if isinstance(res_data, dict):
            return
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
    
    def parse_data(self, res_data):
        if "data" not in res_data.keys():
            return
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
    
    def parse_data(self, res_data):
        if isinstance(res_data, dict) and "message" in res_data.keys():
            return
            # if res_data["message"] == "NotFound":
            #     return
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
        sym = "".join(symbol.split("-"))
        return sym.lower()
    
    def parse_data(self, res_data):
        if res_data["status"] != "ok":
            return
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
    
    # def transform_symbol(self, symbol):
    #     return symbol[:-4] + "-" + "USDT"
    

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
        if "data" not in res_data.keys():
            return
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
        sym = symbol.split("-")
        if sym[1] == "USDT":
            sym[1] = "USD"
        return "/".join(sym)
        # return symbol[:-4] + "/" + "USD"
    
    def construct_url(self, symbol):
        utc_tz = pytz.timezone('UTC')
        starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1) - datetime.timedelta(seconds=5)
        start = int(starttime.timestamp())

        endtime = datetime.datetime.now(tz = utc_tz)
        end = int(endtime.timestamp())

        return self.url.format(symbol, start, end)

    def parse_data(self, res_data):
        if res_data["success"] != True:
            return
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
    
    # def transform_symbol(self, symbol):
    #     return symbol[:-4] + "-" + "USDT"
    
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
    
    # def transform_symbol(self, symbol):
    #     return symbol[:-4] + "-" + "USDT"
    
    def parse_data(self, res_data):
        if "error_message" in res_data:
            return
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
        sym = "".join(symbol.split("-"))
        for record in res_data:
            if record["currency_pair_code"] == sym:
                product_id = record["id"]

        return product_id
    
    def construct_url(self, symbol):
        utc_tz = pytz.timezone('UTC')
        starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1) - datetime.timedelta(seconds=5)
        ts = int(starttime.timestamp())
        return self.url.format(symbol, ts)
    
    def parse_data(self, res_data):
        if "message" in res_data and res_data["message"] == "Product not found":
            return
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
        return "_".join(symbol.split("-"))
        # return symbol[:-4] + "_" + "USDT"
    
    def request_data(self, url):
        try:
            proxies = {"http": "http://5.79.73.131:13150"}
            response = requests.get(url, proxies=proxies)
            if "Invalid input" in response.text:
                return
            return json.loads(response.text)
        except:
            raise RuntimeError
    
    def parse_data(self, res_data):
        if res_data == None:
            return
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
        return "/".join(symbol.split("-"))
        # return symbol[:-4] + "/" + "USDT"
    
    def parse_data(self, res_data):
        if "data" not in res_data:
            return
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
        return "B-" + "_".join(symbol.split("-"))
        # return "B-" + symbol[:-4] + "_" + "USDT"
    
    def request_data(self, url):
        response = requests.get(url)
        if response.text == "":
            return ""
        return json.loads(response.text)
    
    def parse_data(self, res_data):
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            trade_id = str(record["T"]) + str(record["q"])
            quoteQty = float(record["p"]) * float(record["q"])
            data += [trade_id, record["p"], record["q"], quoteQty,  record["T"], record["m"]]
            data[5] = int(int(data[5]) / 1000)
            data_lst.append(data)
        return data_lst


class KrakenTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(KrakenTradeDataCrawler, self).__init__(db_name, interval, symbols, "kraken")
        self.url = "https://api.kraken.com/0/public/Trades?pair={}&since={}"

    def transform_symbol(self, symbol):
        return "".join(symbol.split("-"))

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
        return "/".join(symbol.split("-"))
        # return symbol[:-4] + "/" + "USDT"
    
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
    
    # def transform_symbol(self, symbol):
    #     return symbol[:-4] + "-" + "USDT"
    
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
        self.use_proxy = True
    
    def transform_symbol(self, symbol):
        sym = symbol.split("-")
        if sym[1] == "USDT":
            sym[1] = "USD"
        return "t" + "".join(sym)
        # return "t" + symbol[:-1]

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
        return "_".join(symbol.split("-"))
        # return symbol[:-4] + "_" + "USDT"
    
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

    def transform_symbol(self, symbol):
        return "".join(symbol.split("-"))

    def parse_data(self, res_data):
        if isinstance(res_data, dict):
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
        sym = "".join(symbol.split("-"))
        return sym.lower()
        # return symbol.lower()
    
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
        sym = symbol.split("-")
        if sym[1] == "USDT":
            sym[1] = "USD"
        return "".join(sym)
        # return symbol[:-1]
    
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
        self.use_proxy = True
    
    def transform_symbol(self, symbol):
        sym = symbol.split("-")
        if sym[1] == "USDT":
            sym[1] = "USD"
        return "_".join(sym)
        # return symbol[:-4] + "_" + "USD"
    
    def parse_data(self, res_data):
        # {"id":2235850539,"side":"BUY","price":34108.93,"size":0.12156082,"exec_date":"2021-07-06T23:44:32.76","buy_child_order_acceptance_id":"JRF20210706-234432-301999","sell_child_order_acceptance_id":"JRF20210706-234422-134754"}
        if len(res_data) == 0:
            return
        date2tick = lambda time_str: time.mktime(datetime.datetime.strptime(time_str,
                                       r"%Y-%m-%d %H:%M:%S").timetuple())
        data_lst = []
        try:
            for record in res_data:
                data = [self.exch_name]
                tick = date2tick(record["exec_date"].split(".")[0].replace("T", " "))
                quoteQty = float(record["price"]) * float(record["size"])
                isBuyerMaker = True if record["side"] == "BUY" else False
                data += [record["id"], record["price"], record["size"], quoteQty, tick, isBuyerMaker]
                data_lst.append(data)

            return data_lst
        except Exception as e:
            print(res_data)
            raise e


class PoloniexTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(PoloniexTradeDataCrawler, self).__init__(db_name, interval, symbols, "poloniex")
        self.url = "https://poloniex.com/public?command=returnTradeHistory&currencyPair={}&start={}&end={}"
    
    def transform_symbol(self, symbol):
        sym = symbol.split("-")
        lst = sym[::-1]
        return "_".join(lst)
        # return symbol[-4:] + "_" + symbol[:-4]
    
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
    
    def transform_symbol(self, symbol):
        return "".join(symbol.split("-"))

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
    
class LBankTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(LBankTradeDataCrawler, self).__init__(db_name, interval, symbols, "lbank")
        self.url = "https://api.lbkex.com/v2/trades.do?symbol={}&size=600&time={}"
    def transform_symbol(self, symbol):
        sym = symbol.split("-")
        sym = [k.lower() for k in sym]
        return "_".join(sym)
        # return symbol[:-4].lower() + "_" + symbol[-4:].lower()

    def construct_url(self, symbol):
        now = time.time()
        start_time = int((now - 60 * 1.5) * 1000)
        url = self.url.format(symbol, start_time)
        return url
    
    def parse_data(self, res_data):
        # {"date_ms":1626053370020,"amount":0.1022,"price":34154.29,"type":"sell","tid":"742d9f93c09549079dbaaac7595d4c03"}
        if res_data["result"] == "false":
            return

        data_lst = []
        
        for record in res_data["data"]:
            data = [self.exch_name]
            quoteQty = float(record["price"]) * float(record["amount"])
            tick = int(record["date_ms"] / 1000)
            isBuyerMaker = True if record["type"] == "buy" else False
            data += [record["tid"], record["price"], record["amount"], quoteQty, tick, isBuyerMaker]
            data_lst.append(data)
        
        return data_lst
    

class ItBitTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(ItBitTradeDataCrawler, self).__init__(db_name, interval, symbols, "itBit")
        self.url = "https://api.paxos.com/v2/markets/{}/recent-executions"
    
    def transform_symbol(self, symbol):
        sym = symbol.split("-")
        if sym[1] == "USDT":
            sym[1] = "USD"
        return "".join(sym)
        # return symbol[:-1]
    
    def parse_data(self, res_data):
        if "title" in res_data.keys():
            return
        # {"match_number":"61Y8IHZWWSEO","price":"34201.75","amount":"0.0029092","executed_at":"2021-07-12T02:16:53.446Z"}
        date2tick = lambda time_str: time.mktime(datetime.datetime.strptime(time_str,
                                       r"%Y-%m-%d %H:%M:%S").timetuple())
        data_lst = []
        for record in res_data["items"]:
            data = [self.exch_name]
            quoteQty = float(record["price"]) * float(record["amount"])
            time_str = record["executed_at"].split(".")[0].replace("Z", "").replace("T", " ")
            tick = date2tick(time_str)
            data += [record["match_number"], record["price"], record["amount"], quoteQty, tick, None]
            data_lst.append(data)
        return data_lst


class CoinEXTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(CoinEXTradeDataCrawler, self).__init__(db_name, interval, symbols, "coinEx")
        self.url = "https://api.coinex.com/v1/market/deals?market={}&limit=1000"

    def transform_symbol(self, symbol):
        return "".join(symbol.split("-"))
    
    def parse_data(self, res_data):
        if len(res_data["data"]) == 0:
            return
        # {"id": 2387239331, "type": "buy", "price": "34202.12", "amount": "0.00096480", "date": 1626057780, "date_ms": 1626057780796}
        
        data_lst = []
        for record in res_data["data"]:
            data = [self.exch_name]
            quoteQty = float(record["amount"]) * float(record["price"])
            isBuyerMaker = True if record["type"] == "buy" else False
            data += [record["id"], record["price"], record["amount"], quoteQty, record["date"], isBuyerMaker]
            data_lst.append(data)
        return data_lst


class CEXIOTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(CEXIOTradeDataCrawler, self).__init__(db_name, interval, symbols, "cex.io")
        self.url = "https://cex.io/api/trade_history/{}/"
    
    def transform_symbol(self, symbol):
        return "/".join(symbol.split("-"))
        # return symbol[:-4] + "/" + symbol[-4:]
    
    def parse_data(self, res_data):
        if isinstance(res_data, dict):
            return 
        data_lst = []
        for record in res_data:
            # {"type":"buy","date":"1626136304","amount":"0.770183","price":"14.956","tid":"26621"}
            data = [self.exch_name]
            isBuyerMaker = True if record["type"] == "buy" else False
            quoteQty = float(record["amount"]) * float(record["price"])
            data += [record["tid"], record["price"], record["amount"], quoteQty, record["date"], isBuyerMaker]
            data_lst.append(data)
        return data_lst


class DexTradeTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(DexTradeTradeDataCrawler, self).__init__(db_name, interval, symbols, "dex-trade")
        self.url = "https://api.dex-trade.com/v1/public/trades?pair={}"
    
    def transform_symbol(self, symbol):
        return "".join(symbol.split("-"))
    
    def parse_data(self, res_data):
        if not res_data["status"]:
            return
        # {"volume":0.008029,"rate":32978.74,"price":264.78630346,"timestamp":1626139474,"type":"SELL"}
        data_lst = []
        for record in res_data["data"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["type"] == "BUY" else False
            tid = str(record["timestamp"])
            data += [tid, record["rate"], record["volume"], record["price"], record["timestamp"], isBuyerMaker]
            data_lst.append(data)
        
        return data_lst


class GokuMarketTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(GokuMarketTradeDataCrawler, self).__init__(db_name, interval, symbols, "gokumarket")
        self.url = "https://publicapi.gokumarket.com/trades?currency_pair={}"
    
    def transform_symbol(self, symbol):
        sym = symbol.split("-")
        sym = [k.lower() for k in sym]
        return "_".join(sym)
        # return symbol[:-4].lower() + "_" + symbol[-4:].lower()

    def parse_data(self, res_data):
        if len(res_data) == 0:
            return
        # {"base_volume":0.03654512,"price":33080.73,"quote_volume":1208.9392475376,"timestamp":1626140094644,"tradeID":"btc-usdt-partials-ef00da79-2d0d-4c48-95d8-2ce6dc8c5fd0","type":"sell"}
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            isBuyerMaker = True if record["type"] == "buy" else False
            tick = int(record["timestamp"] / 1000)
            quote = record["quote_volume"] if "quote_volume" in record else None
            data += [record["timestamp"], record["price"], record["base_volume"], quote, tick, isBuyerMaker]
            data_lst.append(data)
        return data_lst


class EQONEXTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(EQONEXTradeDataCrawler, self).__init__(db_name, interval, symbols, "eqonex")
        self.url = "https://eqonex.com/api/getTradeHistory?pairId={}"
        self.pairDct = self.construct_dct()
    
    def construct_dct(self):
        res = requests.get(url="https://eqonex.com/api/getInstrumentPairs")
        jdata = json.loads(res.text)
        dct = {}
        for pair in jdata["instrumentPairs"]:
            dct[pair[1]] = [pair[0], pair[4], pair[5]]
        return dct

    def transform_symbol(self, symbol):
        sym = symbol.split("-")
        if sym[1] == "USDT":
            sym[1] == "USDC"
        key = "/".join(sym)
        # key = symbol[:-4] + "/" + "USDC"
        if key in self.pairDct.keys():
            return self.pairDct[key][0]
        else:
            return -1
    
    def _kernel(self, symbol):
        q_symbol = self.transform_symbol(symbol)
        url = self.construct_url(q_symbol)
        data = self.request_data(url)
        data_lst = self.parse_data(data, symbol)
        self.write_into_db(data_lst, symbol)
    
    def parse_data(self, res_data, symbol):
        if "error" in res_data.keys():
            return
        data_lst = []
        key = symbol[:-4] + "/" + "USDC"
        # [9800,10000000,"20210712-17:50:56.560",5062921,2]
        date2tick = lambda time_str: time.mktime(datetime.datetime.strptime(time_str,
                                       r"%Y%m%d-%H:%M:%S").timetuple())
        
        for record in res_data["trades"]:
            data = [self.exch_name]
            price = float(record[0] / (10 ** self.pairDct[key][1]))
            amount = float(record[1] / (10 **self.pairDct[key][2]))
            quoteQty = price * amount
            tick = date2tick(record[2].split(".")[0])
            data += [record[3], price, amount, quoteQty, tick, 1 & record[4]]
            data_lst.append(data)
        return data_lst


class AAXTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(AAXTradeDataCrawler, self).__init__(db_name, interval, symbols, "aax")
        self.url = "https://api.aax.com/v2/market/trades?symbol={}&limit=1000"
    
    def transform_symbol(self, symbol):
        return "".join(symbol.split("-"))

    def parse_data(self, res_data):
        if res_data["e"] == "empty":
            return
        # "trades":[{"p":"33186.18000000","q":"0.001400","t":1626144220719},
        data_lst = []
        for record in res_data["trades"]:
            data = [self.exch_name]
            tid = str(record["t"])
            o_price = float(record["p"])
            price = abs(o_price)
            isBuyerMaker = True if o_price > 0 else False
            tick = int(record["t"] / 1000)
            q = float(record["q"])
            quoteQty = price * q
            data += [tid, price, q, quoteQty, tick, isBuyerMaker]
            data_lst.append(data)
        return data_lst


class HitBTCTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(HitBTCTradeDataCrawler, self).__init__(db_name, interval, symbols, "hitbtc")
        self.url = "https://api.hitbtc.com/api/2/public/trades/{}?from={}&till&limit=500"
    
    def transform_symbol(self, symbol):
        sym = symbol.split("-")
        if sym[1] == "USDT":
            sym[1] = "USD"
        return "".join(sym)
        # return symbol[:-1]
    
    def construct_url(self, symbol):
        now = time.time()
        start = now - 60 * 1.5
        start = int(start * 1000)
        now = int(now * 1000)
        return self.url.format(symbol, start, now)
    
    def parse_data(self, res_data):
        if isinstance(res_data, dict) and "error" in res_data.keys():
            return
        data_lst = []
        date2tick = lambda time_str: time.mktime(datetime.datetime.strptime(time_str,
                                       r"%Y-%m-%d %H:%M:%S").timetuple())
        # {"id":1317550278,"price":"33223.54","quantity":"0.00005","side":"buy","timestamp":"2021-07-13T03:28:38.289Z"}
        for record in res_data:
            # pdb.set_trace()
            data = [self.exch_name]
            quoteQty = float(record["price"]) * float(record["quantity"])
            isBuyerMaker = True if record["side"] == "buy" else False
            tick = record["timestamp"].split(".")[0].replace("Z", "").replace("T", " ")
            tick = date2tick(tick)
            data += [record["id"], record["price"], record["quantity"], quoteQty, tick, isBuyerMaker]
            data_lst.append(data)
        return data_lst


# class ParitexTradeDataCrawler(BaseExchangeCrawler):
#     ### TODO: This website cannot access by requests
#     def __init__(self, db_name, interval, symbols):
#         super(ParitexTradeDataCrawler, self).__init__(db_name, interval, symbols, "paritex")
#         self.url = "https://www.paritex.com/gateway/api-auth/api-tradeservice/api/v1/public/trade?symbol={}&limit=100"
    
#     def request_data(self, url):
#         s = requests.Session()

#         cookie_dict = {
#             "49BAC005-7D5B-4231-8CEA-16939BEACD67": "cktest001",
#             "JSESSIONID": "F4FFF69B8XXXXXXC8DCB4C061C0",
#             "JSESSIONIDSSO": "9D49C76FD6XXXXXF294242B44A",
#         }
#         # 把cookie值转换为cookiejar类型，然后传给Session
#         s.cookies = requests.utils.cookiejar_from_dict(
#                     cookie_dict, cookiejar=None, overwrite=True
#                     )
#         response = s.get(url=url)
#         pdb.set_trace()
#         return json.loads(response.text)

#     def parse_data(self, res_data):
#         if len(res_data["data"]) == 0:
#             return
#         data_lst = []
#         pdb.set_trace()
#         date2tick = lambda time_str: time.mktime(datetime.datetime.strptime(time_str,
#                                        r"%Y-%m-%d %H:%M:%S").timetuple())
#         for record in res_data["data"]:
#             # {"tradeId":69979420,"quantity":0.01498300,"price":33091.66000000,"createdOn":"2021-07-13T04:07:34.000+0000"}
#             data = [self.exch_name]
#             quoteQty = float(record["price"]) * float(record["quantity"])
#             tick = record["createdOn"].split(".")[0].replace("T", " ")
#             tick = date2tick(tick)
#             data += [record["tradeId"], record["price"], record["quantity"], quoteQty, tick, None]
#             data_lst.append(data)
#         return data_lst

class BigONETradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BigONETradeDataCrawler, self).__init__(db_name, interval, symbols, "bigone")
        self.url = "https://big.one/api/v3/asset_pairs/{}/trades"

    # def transform_symbol(self, symbol):
    #     return symbol[:-4] + "-" + "USDT"

    def parse_data(self, res_data):
        if "message" in res_data.keys():
            return
        data_lst = []
        for record in res_data["data"]:
            data = [self.exch_name]
            quoteQty = float(record["price"]) * float(record["amount"])
            isBuyerMaker = True if record["taker_side"] == "BID" else False
            d = datetime.datetime.strptime(record["created_at"][0:19], "%Y-%m-%dT%H:%M:%S")
            ts = int(d.timestamp())
            data += [record["id"], record["price"], record["amount"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)
        return data_lst

class IndodaxTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(IndodaxTradeDataCrawler, self).__init__(db_name, interval, symbols, "indodax")
        self.url = "https://indodax.com/api/trades/{}"

    def transform_symbol(self, symbol):
        sym = "".join(symbol.split("-"))
        return sym.lower()
        # return symbol.lower()

    def parse_data(self, res_data):
        data_lst = []
        if "error" in res_data:
            return
        for record in res_data:
            data = [self.exch_name]
            quoteQty = float(record["price"]) * float(record["amount"])
            isBuyerMaker = True if record["type"] == "buy" else False
            data += [record["tid"], record["price"], record["amount"], quoteQty, record["date"], isBuyerMaker]
            data_lst.append(data)
        return data_lst

class OkcoinTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(OkcoinTradeDataCrawler, self).__init__(db_name, interval, symbols, "okcoin")
        self.url = "https://www.okcoin.com/api/spot/v3/instruments/{}/trades?limit=100"

    # def transform_symbol(self, symbol):
    #     return symbol[:-4] + "-" + "USDT"

    def parse_data(self, res_data):
        if "error_message" in res_data:
            return
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            quoteQty = float(record["price"]) * float(record["size"])
            isBuyerMaker = True if record["side"] == "buy" else False
            d = datetime.datetime.strptime(record["timestamp"][0:19], "%Y-%m-%dT%H:%M:%S")
            ts = int(d.timestamp())
            data += [record["trade_id"], record["price"], record["size"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)
        return data_lst

class EXMOTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(EXMOTradeDataCrawler, self).__init__(db_name, interval, symbols, "exmo")
        self.url = "https://api.exmo.com/v1.1/trades?pair={}"

    def transform_symbol(self, symbol):
        return "_".join(symbol.split("-"))
        # return symbol[:-4] + "_" + "USDT"

    def parse_data(self, res_data):
        if not isinstance(res_data, dict) or len(res_data) == 0:
            return
        data_lst = []
        symbol = ""
        for key in res_data:
            symbol = key
        for record in res_data[symbol]:
            data = [self.exch_name]
            isBuyerMaker = True if record["type"] == "buy" else False
            data += [record["trade_id"], record["price"], record["quantity"], record["amount"], record["date"], isBuyerMaker]
            data_lst.append(data)
        return data_lst

class BithumbGlobalTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BithumbGlobalTradeDataCrawler, self).__init__(db_name, interval, symbols, "bithumbglobal")
        self.url = "https://global-openapi.bithumb.pro/openapi/v1/spot/trades?symbol={}"

    # def transform_symbol(self, symbol):
    #     return symbol[:-4] + "-" + "USDT"

    def parse_data(self, res_data):
        if res_data["code"] != "0":
            return
        data_lst = []
        for record in res_data["data"]:
            data = [self.exch_name]
            quoteQty = float(record["p"]) * float(record["v"])
            isBuyerMaker = True if record["s"] == "buy" else False
            data += [record["ver"], record["p"], record["v"], quoteQty, record["t"], isBuyerMaker]
            data_lst.append(data)
        return data_lst

class BtcTurkProTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BtcTurkProTradeDataCrawler, self).__init__(db_name, interval, symbols, "btcturkpro")
        self.url = "https://api.btcturk.com/api/v2/trades?pairSymbol={}"

    def transform_symbol(self, symbol):
        return "_".join(symbol.split("-"))
        # return symbol[:-4] + "_" + "USDT"

    def parse_data(self, res_data):
        if res_data["success"] == False:
            return
        data_lst = []
        for record in res_data["data"]:
            data = [self.exch_name]
            quoteQty = float(record["price"]) * float(record["amount"])
            isBuyerMaker = True if record["side"] == "buy" else False
            data += [record["tid"], record["price"], record["amount"], quoteQty, record["date"], isBuyerMaker]
            data[5] = int(data[5] / 1000)
            data_lst.append(data)
        return data_lst

class WootradeTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(WootradeTradeDataCrawler, self).__init__(db_name, interval, symbols, "wootrade")
        self.url = "https://api.woo.network/v1/public/market_trades?symbol={}&limit=50"

    def transform_symbol(self, symbol):
        sym = "_".join(symbol.split("-"))
        return "SPOT_" + sym
        # return "SPOT_" + symbol[:-4] + "_" + "USDT"

    def parse_data(self, res_data):
        data_lst = []
        for record in res_data["rows"]:
            data = [self.exch_name]
            quoteQty = float(record["executed_price"]) * float(record["executed_quantity"])
            isBuyerMaker = True if record["side"] == "BUY" else False
            ts = str(record["executed_timestamp"])
            ts = ts[0:10]
            ts = int(ts)
            tid = str(record["executed_price"]) + str(record["executed_quantity"]) + str(record["executed_timestamp"])
            data += [tid, record["executed_price"], record["executed_quantity"], quoteQty, ts, isBuyerMaker]
            data_lst.append(data)
        return data_lst

class BitfrontTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(BitfrontTradeDataCrawler, self).__init__(db_name, interval, symbols, "bitfront")
        self.url = "https://openapi.bitfront.me/v1/market/public/tradeHistory?coinPair={}&max=50"

    def transform_symbol(self, symbol):
        return ".".join(symbol.split("-"))
        # return symbol[:-4] + "." + "USDT"

    def parse_data(self, res_data):
        if res_data["statusMessage"] != "SUCCESS":
            return
        data_lst = []
        for record in res_data["responseData"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["orderSide"] == "BUY" else False
            data += [record["transactionID"], record["price"], record["amount"], record["totalValue"], record["createdAt"], isBuyerMaker]
            data[5] = int(data[5] / 1000)
            data_lst.append(data)
        return data_lst

class ZBcomTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(ZBcomTradeDataCrawler, self).__init__(db_name, interval, symbols, "zbcom")
        self.url = "https://api.zb.land/data/v1/trades?market={}"

    def transform_symbol(self, symbol):
        sym = symbol.split("-")
        sym = [k.lower() for k in sym]
        return "_".join(sym)
        # return symbol[:-4].lower() + "_" + "usdt"

    def parse_data(self, res_data):
        if "error" in res_data:
            return
        data_lst = []
        for record in res_data:
            data = [self.exch_name]
            isBuyerMaker = True if record["type"] == "buy" else False
            quoteQty = float(record["price"]) * float(record["amount"])
            data += [record["tid"], record["price"], record["amount"], quoteQty, record["date"], isBuyerMaker]
            data_lst.append(data)
        return data_lst

class CoinlistProTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(CoinlistProTradeDataCrawler, self).__init__(db_name, interval, symbols, "coinlistpro")
        self.url = "https://trade-api.coinlist.co/v1/symbols/{}/auctions"

    # def transform_symbol(self, symbol):
    #     return symbol[:-4] + "-" + "USDT"

    def parse_data(self, res_data):
        data_lst = []
        for record in res_data["auctions"]:
            data = [self.exch_name]
            quoteQty = float(record["price"]) * float(record["volume"])
            d = datetime.datetime.strptime(record["logical_time"][0:19], "%Y-%m-%dT%H:%M:%S")
            ts = int(d.timestamp())
            data += [record["auction_code"], record["price"], record["volume"], quoteQty, ts, None]
            data_lst.append(data)
        return data_lst

class PairbuTradeDataCrawler(BaseExchangeCrawler):
    def __init__(self, db_name, interval, symbols):
        super(PairbuTradeDataCrawler, self).__init__(db_name, interval, symbols, "pairbu")
        self.url = "https://v3.paribu.com/app/markets/{}?interval=1000"

    def transform_symbol(self, symbol):
        return symbol[:-4].lower() + "-" + "usdt"

    def parse_data(self, res_data):
        if res_data["success"] == False:
            return
        data_lst = []
        for record in res_data["data"]["marketMatches"]:
            data = [self.exch_name]
            isBuyerMaker = True if record["trade"] == "buy" else False
            quoteQty = float(record["price"]) * float(record["amount"])
            tid = str(record["timestamp"]) + str(record["price"]) + str(record["amount"])
            data += [tid, record["price"], record["amount"], quoteQty, record["timestamp"], isBuyerMaker]
            data_lst.append(data)
        return data_lst