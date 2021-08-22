import pdb
import json
import time
import datetime
import pytz
from datetime import timezone

from tqdm import tqdm

import requests
from .mysql_connector import CryptoCoinConnector
from .print_section import print_error_sleep, print_sleep, print_write_data, print_error_pair


class BaseCrawler():
    """Base abstract of Crawler"""

    def __init__(self, db_name):
        k = 500
        self.load_top_k_pair(k)
        db_names = []
        for i in range(int(k / 10) - 1):
            lower = i * 10 + 1
            upper = lower + 9
            db_name = "historical_Klines_top{}_{}coins"
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
            db_name = "historical_Klines_top{}_{}coins"
            db_name = db_name.format(str(lower), str(upper))
            symbol = dct[str(t + 1)] + "_USDT"
            self.db_name_dict[symbol] = db_name

    def handle_network_issue(self, function, *kwargs):
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


class BaseKlineCrawler(BaseCrawler):
    # flag = true represents the crawler need to crawl step by step
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp, exch_name, flag = False):
        super(BaseKlineCrawler, self).__init__(db_name)
        # self.start_timestamp = 1532398889
        # self.end_timestamp = 1627006889
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.exch_name = exch_name
        self.flag = flag
        self.exch_name = exch_name

        self.symbols = symbols
    
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
        if data_lst == None or len(data_lst) == 0:
            return
        db_name = self.db_name_dict[symbol]
        self.connector.use_database(db_name)

        # divide the data_lst into data_group with same table_index, and insert them into database
        while len(data_lst) != 0:
            table_index = int(int(data_lst[0][6]) / 2592000)

            data_group = []
            data_group.append(data_lst[0])
            del data_lst[0]

            j = 0
            for i in range(len(data_lst)):
                index = int(int(data_lst[j][6]) / 2592000)
                if table_index == index:
                    data_group.append(data_lst[j])
                    del data_lst[j]
                else:
                    j = j + 1

            self.connector.create_Kline_info_table(symbol, table_index, table_index)
            self.connector.insert_Kline_data(data_group, symbol, table_index)

    def _kernel(self, symbol, start_timestamp, end_timestamp):
        try:
            q_symbol = self.transform_symbol(symbol)
            url = self.construct_url(q_symbol, start_timestamp, end_timestamp)
            data = self.request_data(url)
            data_lst, start_timestamp, end_timestamp, size = self.parse_data(data, start_timestamp, end_timestamp)

            new_start = int(str(start_timestamp)[0:10])
            new_end = int(str(end_timestamp)[0:10])
            print(symbol)
            print(new_start / 2592000)
            print(new_end / 2592000)
            print(size)

            self.write_into_db(data_lst, symbol)
            return start_timestamp, end_timestamp, size
        except Exception as e:
            raise e

    def run(self, test_flag=False):
        """Kernel function"""

        i = 1
        flag = self.flag
        for symbol in self.symbols:
            if flag:
                start_timestamp = self.start_timestamp
                end_timestamp = start_timestamp + 59940
            else:
                start_timestamp = self.start_timestamp
                end_timestamp = self.end_timestamp

            while True:
                print_write_data()
                if i % self.rate_limit == 0:
                    print_sleep(self.interval)
                    time.sleep(self.interval)
                
                try:
                    start_timestamp, end_timestamp, size = self._kernel(symbol, start_timestamp, end_timestamp)
                except Exception as e:
                    self.handle_network_issue(e, self._kernel, symbol)

                if flag:
                    if start_timestamp > self.end_timestamp:
                        break
                else:
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


class BinanceKlineDataCrawler(BaseKlineCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(BinanceKlineDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "binance")
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
        if "msg" in res_data:
            return data_lst, start_timestamp, end_timestamp, 0

        for record in res_data:
            data = [self.exch_name]
            Open = (float(record[1]))
            High = (float(record[2]))
            Low = (float(record[3]))
            Close = (float(record[4]))
            Volume = float(record[5])
            opentime = int(record[0]) / 1000
            data += [Open, Close, High, Low, Volume, opentime]
            data_lst.append(data)

        size = len(res_data)
        if size != 0:
            start_timestamp = res_data[-1][6]

        return data_lst, start_timestamp, end_timestamp, size

class FTXKlineDataCrawler(BaseKlineCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(FTXKlineDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "ftx")
        self.url = "https://ftx.com/api/markets/{}/candles?resolution=60&start_time={}&end_time={}"
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
            Open = (float(record["open"]))
            High = (float(record["high"]))
            Low = (float(record["low"]))
            Close = (float(record["close"]))
            Volume = float(record["volume"])
            opentime = int(int(record["time"]) / 1000)
            data += [Open, Close, High, Low, Volume, opentime]
            data_lst.append(data)

        size = len(res_data["result"])
        if size != 0:
            # find the smallest timestamp to be the new end_timestamp
            end_timestamp = int(res_data["result"][0]["time"]) / 1000 - 1

        return data_lst, start_timestamp, end_timestamp, size

class FTXUSKlineDataCrawler(BaseKlineCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(FTXUSKlineDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "ftxus")
        self.url = "https://ftx.us/api/markets/{}/candles?resolution=60&start_time={}&end_time={}"
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
            Open = (float(record["open"]))
            High = (float(record["high"]))
            Low = (float(record["low"]))
            Close = (float(record["close"]))
            Volume = float(record["volume"])
            opentime = int(record["time"]) / 1000
            data += [Open, Close, High, Low, Volume, opentime]
            data_lst.append(data)

        size = len(res_data["result"])
        if size != 0:
            # find the smallest timestamp to be the new end_timestamp
            end_timestamp = int(res_data["result"][0]["time"]) / 1000 - 1

        return data_lst, start_timestamp, end_timestamp, size

class KucoinKlineDataCrawler(BaseKlineCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(KucoinKlineDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "kucoin")
        self.url = "https://api.kucoin.com/api/v1/market/candles?symbol={}&startAt={}&endAt={}&type=1min"
        self.rate_limit = 29
        self.interval = 1
    
    def transform_symbol(self, symbol):
        pair = symbol.split("_")
        return pair[0] + "-" + pair[1]

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if int(res_data["code"]) != 200000 or len(res_data["data"]) == 0:
            return data_lst, start_timestamp, end_timestamp, 0

        for record in res_data["data"]:
            data = [self.exch_name]
            Open = float(record[1])
            High = float(record[3])
            Low = float(record[4])
            Close = float(record[2])
            Volume = float(record[6])
            opentime = int(record[0])
            data += [Open, Close, High, Low, Volume, opentime]
            data_lst.append(data)

        size = len(data_lst)
        if size != 0:
            # find the smallest timestamp to be the new end_timestamp
            end_timestamp = int(res_data["data"][-1][0]) - 1

        return data_lst, start_timestamp, end_timestamp, size

class BinanceUSKlineDataCrawler(BaseKlineCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(BinanceUSKlineDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "binanceus")
        self.url = "https://api.binance.us/api/v3/klines?symbol={}&interval=1m&startTime={}&endTime={}"
        self.rate_limit = 29
        self.interval = 1

    def transform_symbol(self, symbol):
        pair = symbol.split("_")
        return pair[0] + pair[1]

    def construct_url(self, symbol, start_timestamp, end_timestamp):
        return self.url.format(symbol, start_timestamp, end_timestamp)

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if "msg" in res_data:
            return data_lst, start_timestamp, end_timestamp, 0

        for record in res_data:
            try:
                data = [self.exch_name]
                Open = (float(record[1]))
                High = (float(record[2]))
                Low = (float(record[3]))
                Close = (float(record[4]))
                Volume = float(record[5])
                opentime = int(record[0]) / 1000
                data += [Open, Close, High, Low, Volume, opentime]
                data_lst.append(data)
            except:
                continue

        size = len(res_data)
        if size != 0:
            start_timestamp = res_data[-1][6]

        return data_lst, start_timestamp, end_timestamp, size

class GateIOKlineDataCrawler(BaseKlineCrawler):
    def __init__(self, db_name, symbols, start_timestamp, end_timestamp):
        super(GateIOKlineDataCrawler, self).__init__(db_name, symbols, start_timestamp, end_timestamp, "gateio", flag = True)
        self.url = "https://api.gateio.ws/api/v4/spot/candlesticks?currency_pair={}&interval=1m&from={}&to={}"
        self.rate_limit = 200
        self.interval = 1

    def transform_symbol(self, symbol):
        pair = symbol.split("_")
        return pair[0] + "_" + pair[1]

    def construct_url(self, symbol, start_timestamp, end_timestamp):
        return self.url.format(symbol, start_timestamp, end_timestamp)

    def parse_data(self, res_data, start_timestamp, end_timestamp):
        data_lst = []
        if isinstance(res_data, list) and len(res_data) == 0 or isinstance(res_data, dict):
            start_timestamp += 10000
            end_timestamp += 10000
            return data_lst, start_timestamp, end_timestamp, 0

        for record in res_data:
            data = [self.exch_name]
            Open = (float(record[5]))
            High = (float(record[3]))
            Low = (float(record[4]))
            Close = (float(record[2]))
            Volume = float(record[1])
            opentime = int(record[0])
            data += [Open, Close, High, Low, Volume, opentime]
            data_lst.append(data)

        
        start_timestamp += 10000
        end_timestamp += 10000

        size = len(res_data)

        return data_lst, start_timestamp, end_timestamp, size