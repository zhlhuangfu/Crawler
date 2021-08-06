import pdb
import time
import json
import random

import argparse

from Modules.mysql_connector import InitTable, CryptoCoinConnector
from Modules.print_section import print_sleep, print_write_data
from Modules.crawler import (
    BinanceTradeDataCrawler, BitfinexTradeDataCrawler, BitflyerTradeDataCrawler,
    BitstampTradeDataCrawler, BinanceUSTradeDataCrawler, CoinbaseTradeDataCrawler,
    FTXTradeDataCrawler, GateIOTradeDataCrawler, GeminiTradeDataCrawler,
    HuobiTradeDataCrawler, KrakenTradeDataCrawler, KucoinTradeDataCrawler,
    PoloniexTradeDataCrawler, 
    AscendEXTradeDataCrawler, BittrexTradeDataCrawler,
    CoinDCXTradeDataCrawler, CryptoComExchangeTradeDataCrawler, FTXUSTradeDataCrawler,
    HuobiKoreaTradeDataCrawler, LiquidTradeDataCrawler, OKExTradeDataCrawler, ProBitGlobalTradeDataCrawler,

    LBankTradeDataCrawler, ItBitTradeDataCrawler, CoinEXTradeDataCrawler, CEXIOTradeDataCrawler,
    DexTradeTradeDataCrawler, GokuMarketTradeDataCrawler, EQONEXTradeDataCrawler, AAXTradeDataCrawler,
    HitBTCTradeDataCrawler, BigONETradeDataCrawler, IndodaxTradeDataCrawler, OkcoinTradeDataCrawler,
    EXMOTradeDataCrawler, BithumbGlobalTradeDataCrawler, BtcTurkProTradeDataCrawler,
    WootradeTradeDataCrawler, BitfrontTradeDataCrawler, ZBcomTradeDataCrawler, CoinlistProTradeDataCrawler
)


exch_dct = {
    "binance" : BinanceTradeDataCrawler,
    "binance.us" : BinanceUSTradeDataCrawler,
    "bitfinex" : BitfinexTradeDataCrawler,
    "bitflyer": BitflyerTradeDataCrawler,
    "bitstamp": BitstampTradeDataCrawler,
    "coinbase": CoinbaseTradeDataCrawler,
    "ftx": FTXTradeDataCrawler,
    "gate.io": GateIOTradeDataCrawler,
    "gemini": GeminiTradeDataCrawler,
    "huobi": HuobiTradeDataCrawler,
    "kraken": KrakenTradeDataCrawler,
    "kucoin": KucoinTradeDataCrawler,
    "poloniex": PoloniexTradeDataCrawler,
    "ascendEX": AscendEXTradeDataCrawler,
    "bittrex": BittrexTradeDataCrawler,
    "coinDCX": CoinDCXTradeDataCrawler,
    "cryptoComExchange": CryptoComExchangeTradeDataCrawler,
    "ftxUS": FTXUSTradeDataCrawler,
    "huobiKorea": HuobiKoreaTradeDataCrawler,
    "liquid": LiquidTradeDataCrawler,
    "OKEx": OKExTradeDataCrawler,
    "proBitGlobal": ProBitGlobalTradeDataCrawler,

    "Lbank":LBankTradeDataCrawler,
    "ItBit":ItBitTradeDataCrawler,
    "CoinEX": CoinEXTradeDataCrawler,
    "CEXIO": CEXIOTradeDataCrawler,
    "Dex": DexTradeTradeDataCrawler,
    "GokuMarket": GokuMarketTradeDataCrawler,
    "EQONext": EQONEXTradeDataCrawler,
    "AAX": AAXTradeDataCrawler,
    "HitBTC": HitBTCTradeDataCrawler,
    "BigOne": BigONETradeDataCrawler,
    "Indodax": IndodaxTradeDataCrawler,
    "Okcoin": OkcoinTradeDataCrawler,
    "EXMO": EXMOTradeDataCrawler,
    "BithumbGlobal": BithumbGlobalTradeDataCrawler,
    "BtcTurkPro": BtcTurkProTradeDataCrawler,
    "Wootrade": WootradeTradeDataCrawler,
    "Bitfront": BitfrontTradeDataCrawler,
    "ZBcom": ZBcomTradeDataCrawler,
    "CoinlistPro": CoinlistProTradeDataCrawler
}


interval = 1
db_name = "trade_info"
# db_name = "TEST"

exchs = ["binance", "coinbase", "huobi", "ftx", "kraken", 
    "kucoin", "bitfinex", "binance.us", "gate.io", "bitstamp",
    "gemini", "bitflyer", "poloniex", "bittrex", "ftxUS"
    ]

# na = ['ETHUSDT', 'USDTUSDT', 'ADAUSDT', 'USDCUSDT', 'DOTUSDT', 'UNIUSDT', 'BUSDUSDT', 'LINKUSDT', 'WBTCUSDT', 'MATICUSDT', 'XLMUSDT', 'ETCUSDT', 'THETAUSDT', 'ICPUSDT', 'VETUSDT', 'DAIUSDT', 'FILUSDT', 'TRXUSDT', 'LUNAUSDT', 'XMRUSDT', 'AAVEUSDT', 'EOSUSDT', 'CAKEUSDT', 'FTTUSDT', 'CROUSDT', 'BTCBUSDT', 'AMPUSDT', 'MKRUSDT', 'LEOUSDT', 'MIOTAUSDT', 'BSVUSDT', 'ALGOUSDT', 'XTZUSDT', 'AXSUSDT', 'COMPUSDT', 'DCRUSDT', 'USTUSDT', 'HBARUSDT', 'BTTUSDT', 'QNTUSDT', 'WAVESUSDT', 'KSMUSDT', 'TFUELUSDT', 'DASHUSDT', 'EGLDUSDT', 'RUNEUSDT', 'CELUSDT', 'HNTUSDT', 'TUSDUSDT', 'MANAUSDT', 'ENJUSDT', 'YFIUSDT', 'OKBUSDT', 'SNXUSDT', 'FLOWUSDT', 'NEXOUSDT', 'TELUSDT', 'NEARUSDT', 'BATUSDT', 'XDCUSDT', 'ZILUSDT', 'SCUSDT', 'KCSUSDT', 'CELOUSDT', 'QTUMUSDT', 'CHSBUSDT', 'ONTUSDT', 'ANKRUSDT', 'DGBUSDT', 'ICXUSDT', 'ZRXUSDT', 'ZENUSDT', 'MDXUSDT', 'CRVUSDT', 'FTMUSDT', 'OMGUSDT']


def init_top():
    with open("sorted_top_500_symbols.json") as fi:
        dct = json.load(fi)
    symbols_lst = list(dct.values())
    symbols = [sbl + "USDT" for sbl in symbols_lst]
    InitTable(db_name, symbols, exchs)


def load_top_k(k):
    with open("sorted_top_500_symbols.json") as fi:
        dct = json.load(fi)
    sym_lst = []
    for t in range(k):
        sym_lst.append(dct[str(t + 1)])
    return sym_lst


def split_coins(k, batch=4):
    symbols = load_top_k(500)
    random.seed(10)
    random.shuffle(symbols)
    symbols = [sbl + "-USDT" for sbl in symbols]
    N = len(symbols)
    st = k * batch
    ed = st + batch
    if ed > N:
        ed = N
    return symbols[st:ed]


def top100_crawl(kth):
    symbols = split_coins(kth)
    # symbols =  [sym[:-4] + "-" + sym[-4:] for sym in na]
    print("Processing {}".format(symbols))
    while True:
        print_write_data()
        for exch in exchs:
            spider = exch_dct[exch](db_name, interval, symbols)
            spider.set_verbose(False)
            spider.run(test_flag=True)
            
        print_sleep(interval)
        time.sleep(interval)


if __name__ == "__main__":
    # init_top()
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--kth', type=int,
                    help='an name for the exchange')
    args = parser.parse_args()
    top100_crawl(args.kth)