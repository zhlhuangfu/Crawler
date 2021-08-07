import os
import pdb
import time
import json
from sklearn.cluster import KMeans

import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import timedelta as td
from requests import Session

from Modules.mysql_connector import CryptoCoinConnector


def crawl_data(url, params):
    '''
    Get data from CMC. 
    A free account of CMC can get a free api_key to access public data.
    Each calling of this method, will consume some credits.
    The free api_key support about 300 credits / day.
    Different endpoint will consume different credits.

    url: The endpoint provided by CMC
    params: related parameters
    '''

    sess = Session()
    api_key = "a0d351a5-6dd8-46e8-a25f-59d813bd267d"
    headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': api_key,
    }
    sess.headers.update(headers)
    res = sess.get(url, params=params)
    sess.close()
    return json.loads(res.text)


def get_top_k_ids_dct(k=100):
    '''
    Get top 100 coins from the mapping of CMC
    '''

    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
    params = {"limit": 5000, "start" : 1}
    data1 = crawl_data(url, params)
    params["start"] = 5001
    data2 = crawl_data(url, params)
    
    top_k_dct = {}
    rank2id_dct = {}
    for item in data1["data"]:
        if item["rank"] <= k:
            top_k_dct[str(item["symbol"])] = item["name"]
            rank2id_dct[str(item["rank"])] = item["id"]
        
    for item in data2["data"]:
        if item["rank"] <= k:
            top_k_dct[str(item["symbol"])] = item["name"]
            rank2id_dct[str(item["rank"])] = item["id"]
    return top_k_dct, rank2id_dct


def str2timestamp(time_str: str) -> float:
    """Convert string to timestp"""
    if len(time_str) > 11:
        return time.mktime(
            datetime.datetime.strptime(time_str,
                                       r"%Y-%m-%d %H:%M:%S").timetuple())


def get_price(symbols, exchs=None):
    db_name = "trade_info"
    MySQLConnector = CryptoCoinConnector(db_name)
    start = str2timestamp("2021-8-4 08:00:00")
    end = str2timestamp("2021-8-5 08:00:00")

    prices = []
    for symbol in tqdm(symbols):
        if exchs is not None:
            trades = MySQLConnector.look_up_trade_in_top_15(symbol, start, end, exchs)
        else:
            trades = MySQLConnector.look_up_trade_info(symbol, start, end)
        with open("logs/{}.txt".format(symbol), "w") as fi:
            for line in tqdm(trades):
                fi.write(str(line) + "\n")

    MySQLConnector.close()


def parse_data(symbol):
    fname = "logs/{}.txt".format(symbol)
    lst = []
    with open(fname) as fi:
        for line in fi:
            line = line.split(",")
            data = [float(line[2]), float(line[3]), int(line[5])]
            lst.append(data)
    if len(lst) == 0:
        return
    df = pd.DataFrame(lst)
    df = df.rename(columns={0:"price", 1:"amount", 2:"time"})
    df = df.sort_values(by=["time"]).reset_index(drop=True)

    df.to_csv(csn, index=False)


def agg_price(symbols, date):    
    for symbol in tqdm(symbols):
        if symbol == "USDTUSDT": continue
        st, ed = None, None
        hf = pd.DataFrame()
        fname = "cluster/{}.csv".format(symbol)
        if not os.path.exists(fname): continue
        try:
            df = pd.read_csv(fname)
        except:
            continue
        agg_fn = "cluster/AGG_{}.csv".format(symbol)
        if os.path.exists(agg_fn): continue
        lst = []
        for i in range(288):
            st = date + i * td(minutes=5)
            ed = date + (i + 1) * td(minutes=5)
            st_stp = time.mktime(st.timetuple())
            ed_stp = time.mktime(ed.timetuple())
            tmp = df[(df["time"] >= st_stp) & (df["time"] < ed_stp)]
            agg = tmp["price"] * tmp["amount"]
            P = agg.sum() / tmp["amount"].sum()
            lst.append(P)
        hf[symbol] = lst
        hf.to_csv(agg_fn, index=False)


def conbine(symbols):
    df = pd.DataFrame()
    for symbol in symbols:
        fn = "cluster/AGG_{}.csv".format(symbol)
        if not os.path.exists(fn):  continue
        tf = pd.read_csv(fn)
        val = tf[symbol].values
        pre = None
        lst = []
        for v in val:
            if v == "" or np.isnan(v):
                if pre == None:
                    v = 0
                else:
                    v = pre
            else:
                pre = float(v)
            lst.append(float(v))
        arr = np.array(lst)
        df[symbol] = arr[1:] - arr[:-1]
       
    feats = np.transpose(df.values)
    km = KMeans(4)
    km.fit(feats)
    print(km.labels_)


if __name__ == "__main__":
    # print("Get symbols")
    # topk_dct, _ = get_top_k_ids_dct(100)
    # symbs = [k + "USDT" for k in topk_dct.keys()]

    # exchs = ["binance", "coinbase", "huobi", "ftx", "kraken", 
    # "kucoin", "bitfinex", "binance.us", "gate.io", "bitstamp",
    # "gemini", "bitflyer", "poloniex", "bittrex", "ftxUS"]
    # print("Get prices")
    # get_price(symbs, exchs)
    symbols = []
    with open("sorted_top_500_symbols.json") as fi:
        dct = json.load(fi)
    for i in range(1, 101):
        symbols.append(dct[str(i)] + "USDT")
    
    # for syb in tqdm(symbols):
    #     csn = "cluster/{}.csv".format(syb)
    #     if os.path.exists(csn):
    #         continue
    #     parse_data(syb)
    # dat = datetime.datetime(2021, 8, 4, 8, 0, 0)
    # agg_price(symbols, dat)
    conbine(symbols)
    