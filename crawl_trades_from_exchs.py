import pdb
import json
import argparse

from Modules.crawler import (
    BinanceTradeDataCrawler, BitfinexTradeDataCrawler, BitflyerTradeDataCrawler,
    BitstampTradeDataCrawler, BinanceUSTradeDataCrawler, CoinbaseTradeDataCrawler,
    FTXTradeDataCrawler, GateIOTradeDataCrawler, GeminiTradeDataCrawler,
    HuobiTradeDataCrawler, KrakenTradeDataCrawler, KucoinTradeDataCrawler,
    PoloniexTradeDataCrawler)

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
    "poloniex": PoloniexTradeDataCrawler
}

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--exch_name', type=str,
                    help='an name for the exchange')

args = parser.parse_args()

interval = 60
db_name = "trade_info"
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]
if args.exch_name == "kraken":
    interval = 30
spider = exch_dct[args.exch_name](db_name, interval, symbols)
spider.run()