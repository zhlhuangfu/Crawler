# # import ccxt
# # print(ccxt.exchanges)
# # okex = ccxt.okex()
# # okex.load_markets()
# # if okex.has['fetchTrades']:
# #     print(okex.markets)

import requests
import datetime
import time
import pytz
import json
# # url = "https://api.probit.com/api/exchange/v1/trade?market_id={}&start_time=2019-03-19T02:00:00.000Z&end_time=2019-03-19T03:00:00.000Z&limit=1000"
# # url = url.format('BTC-USDT')
# # response = requests.get(url)
# # print(response.text)
# # utc_tz = pytz.timezone('UTC')
# # time = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1)
# # # print(time.timestamp())
# # time = time.isoformat()

# # s = str(time)
# # s = s[0:23]
# # s = s + 'Z'


# # d = datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")
# # t = d.timestamp()
# # print(int(t))


# url = "https://api.bittrex.com/v3/markets/BTC-USDT/trades"

# # utc_tz = pytz.timezone('UTC')
# # starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1)
# # start = int(starttime.timestamp())

# # endtime = datetime.datetime.now(tz = utc_tz)
# # end = int(endtime.timestamp())

# # url = url.format(start, end)
# response = requests.get(url)

# j = json.loads(response.text)
# time = j[0]["executedAt"][0:19]
# # d = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%f")
# # ts = int(d.timestamp())
# print(time)

from Modules.mysql_connector import CryptoCoinConnector

db_name = "trade_info"
MySQLConnector = CryptoCoinConnector(db_name)

utc_tz = pytz.timezone('UTC')
starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1)
start = int(starttime.timestamp())
endtime = datetime.datetime.now(tz = utc_tz)
end = int(endtime.timestamp())

symbol = "BTCUSDT"

trades = MySQLConnector.look_up_trade_info(symbol, start, end)

P1 = 0
Q1 = 0
P2 = 0
Q2 = 0
for trade in trades:
    p = trade[2]
    q = trade[3]

    P1 = P1 + p * q
    Q1 = Q1 + q

    P2 = P2 + p * p * q
    Q2 = Q2 + p * q

Price1 = P1 / Q1
Price2 = P2 / Q2

print(Price1)
print(Price2)
