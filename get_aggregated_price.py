import datetime
import pytz
from Modules.mysql_connector import CryptoCoinConnector

db_name = "trade_info"
MySQLConnector = CryptoCoinConnector(db_name)

symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "LTCUSDT", "DOTUSDT", "UNIUSDT", "BCHUSDT"]

utc_tz = pytz.timezone('UTC')
starttime = datetime.datetime.now(tz = utc_tz)-datetime.timedelta(minutes=1)
start = int(starttime.timestamp())
endtime = datetime.datetime.now(tz = utc_tz)
end = int(endtime.timestamp())

for symbol in symbols:
    trades = MySQLConnector.look_up_trade_info(symbol, start, end)

    P = 0
    Q = 0
    for trade in trades:
        p = trade[2]
        q = trade[3]
        P += p * q
        Q += q

    AggregatedPrice = P / Q
    
    print("aggregated price of " + symbol + " is: " + str(AggregatedPrice))