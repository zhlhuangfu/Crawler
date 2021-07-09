import pdb

from influxdb_client import BucketsApi
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS




class BaseInfluxConnector:
    def __init__(self, url, token, org, bucket):
        self.url = url
        self.token = token
        self.bucket = bucket
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.writer = self.client.write_api(write_options=SYNCHRONOUS)
        self.reader = self.client.query_api()
    

class CryptoInfluxConnector(BaseInfluxConnector):
    def __init__(self, bucket="CryptoExchange"):
        super(CryptoInfluxConnector, self).__init__(
            url="http://localhost:8086",
            token="c3yIMABCksGxu198Az52W4O9pgd3-sHq7HH08j6r82DCpBriBkvAcT3uBRxhcagHkJF6DAwuRrgi-y_jHGspJQ==",
            org="nus", bucket=bucket)
    
    def write_price(self, symbol, price):
        p = Point(symbol).tag("pair", symbol).field("price", price)
        self.writer.write(bucket=self.bucket, record=p)
    

    def query_price(self, symbol, interval="-1m"):
        q_str = '''from (bucket: "{}") |> range(start: {})'''.format(self.bucket, interval)
        q_str += '''|> filter (fn: (r) => r["_measurement"] == "{}")'''.format(symbol)
        tables = self.reader.query(q_str)
        lst = []
        for tab in tables:
            for row in tab:
                lst.append(row.values)
        return lst
