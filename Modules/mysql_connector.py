"""This module is to help to connect mysql DB"""
import pdb
import time
import pandas as pd
import numpy as np
from tqdm import tqdm
import mysql.connector as mq


class BaseMySQLConnector():
    """Base abstract to connect to MySQL"""

    def __init__(self, db_names):
        """Init"""
        self.db = BaseMySQLConnector.create_connection()
        self.create_databases(db_names)

    def create_databases(self, db_names):
        """Construct SQL to create DB"""
        for db_name in db_names:
            sql = "CREATE DATABASE IF NOT EXISTS {}".format(db_name)
            self.exe_sql(sql)

    def create_database(self, db_name):
        """Construct SQL to create DB"""
        sql = "CREATE DATABASE IF NOT EXISTS {}".format(db_name)
        self.exe_sql(sql)

    def use_database(self, db_name):
        self.exe_sql("USE {}".format(db_name))

    @classmethod
    def create_connection(cls):
        """Create connection to DB"""
        try:
            return mq.connect(host="localhost", user="root", passwd="123456")
        except Exception as e:
            raise e

    def fetch_sql_res(self, sql, vals):
        """Execute SQL and return the results"""
        try:
            cursor = self.db.cursor()
            cursor.execute(sql, vals)
            res = cursor.fetchall()
            cursor.close()
            return res
        except Exception as e:
            raise e

    def exe_sql(self, sql):
        """Execute the SQL"""
        try:
            cursor = self.db.cursor()
            cursor.execute(sql)
            cursor.close()
            self.db.commit()
        except Exception as e:
            raise e

    def exe_sql_many(self, sql, vals):
        """Execute SQL with many values"""
        try:
            cursor = self.db.cursor()
            cursor.executemany(sql, vals)
            cursor.close()
            self.db.commit()
        except Exception as e:
            raise e

    def close(self):
        """Disconnect"""
        self.db.close()


class CryptoCoinConnector(BaseMySQLConnector):
    """Extended class for crawling CryptoCoin"""

    def create_meta_table(self):
        """Construct meta table"""
        sql = "CREATE TABLE IF NOT EXISTS exchange_meta"
        sql += " (exch_name VARCHAR(255), symbol VARCHAR(255), add_time INT, PRIMARY KEY (exch_name, symbol))"
        self.exe_sql(sql)
        print("Meta table created!")
    
    def insert_meta_data(self, data):
        """Insert meta data"""
        sql = "INSERT IGNORE INTO exchange_meta (exch_name, symbol, add_time) VALUES (%s, %s, %s)"
        self.exe_sql_many(sql, data)
        print("Insert meta data success!")

    def create_trade_info_table(self, symbol, start, end):
        """Create table of trade by tick in binance"""
        for i in range(start, end + 1):
            table_name = "trade_of_{}_{}".format(symbol, i)
            sql = "CREATE TABLE IF NOT EXISTS {}".format(table_name)
            sql += " (exch_name VARCHAR(255), id VARCHAR(255), price DOUBLE, qty DOUBLE, quoteQty DOUBLE, time BIGINT(8) unsigned, isBuyerMaker BOOL, PRIMARY KEY (exch_name, id))"
            self.exe_sql(sql)
            # print("table {} Created Successfully!".format(table_name))

    def create_Kline_info_table(self, symbol, start, end):
        """Create table of trade by tick in binance"""
        for i in range(start, end + 1):
            table_name = "Kline_of_{}_{}".format(symbol, i)
            sql = "CREATE TABLE IF NOT EXISTS {}".format(table_name)
            sql += " (exch_name VARCHAR(255), open DOUBLE, close DOUBLE, high DOUBLE, low DOUBLE, volume DOUBLE, time BIGINT(8) unsigned, PRIMARY KEY (exch_name, time))"
            self.exe_sql(sql)
            # print("table {} Created Successfully!".format(table_name))
            
    def create_price_info_table(self, symbol, start, end):
        """Create table of trade by tick in binance"""
        for i in range(start, end + 1):
            table_name = "price_of_{}_{}".format(symbol, i)
            sql = "CREATE TABLE IF NOT EXISTS {}".format(table_name)
            sql += " (price DOUBLE, amount DOUBLE, volume DOUBLE, time BIGINT(8) unsigned, PRIMARY KEY (time))"
            self.exe_sql(sql)
            print("table {} Created Successfully!".format(table_name))

    def insert_trade_data(self, data, symbol, table_index):
        table_name = "trade_of_{}_{}".format(symbol, table_index)
        sql = "INSERT IGNORE INTO {} (exch_name, id, price, qty, quoteQty, time, isBuyerMaker) ".format(table_name)
        sql += "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        self.exe_sql_many(sql, data)

    def insert_Kline_data(self, data, symbol, table_index):
        table_name = "Kline_of_{}_{}".format(symbol, table_index)
        sql = "INSERT IGNORE INTO {} (exch_name, open, close, high, low, volume, time) ".format(table_name)
        sql += "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        self.exe_sql_many(sql, data)

    def insert_price_data(self, data, symbol, table_index):
        table_name = "price_of_{}_{}".format(symbol, table_index)
        sql = "INSERT IGNORE INTO {} (price, amount, volume, time) ".format(table_name)
        sql += "VALUES (%s, %s, %s, %s)"
        self.exe_sql_many(sql, data)

    def look_up_trade_info(self, symbol, start_time, end_time):
        start = int(int(str(start_time)[0:10]) / 2592000)
        end = int(int(str(end_time)[0:10]) / 2592000)

        res = []
        for i in range(start, end + 1):
            table_name = "trade_of_{}_{}".format(symbol, i)
            sql = "SELECT * from "
            sql += table_name
            sql += " where time >= %s and time <= %s"
            res += self.fetch_sql_res(sql, (start_time, end_time))
        
        return res

    def look_up_Kline_info(self, symbol, start_time, end_time):
        start = int(int(str(start_time)[0:10]) / 2592000)
        end = int(int(str(end_time)[0:10]) / 2592000)

        res = []
        for i in range(start, end + 1):
            table_name = "Kline_of_{}_{}".format(symbol, i)
            sql = "SELECT * from "
            sql += table_name
            sql += " where time >= %s and time <= %s"
            res += self.fetch_sql_res(sql, (start_time, end_time))
        
        return res
    
    def look_up_trade_in_top_15(self, symbol, start_time, end_time, top_15):
        table_name = "trade_of_{}".format(symbol)
        sql = "SELECT * from "
        sql += table_name
        sql += " where time > %s and time < %s and exch_name in (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s)"
        return self.fetch_sql_res(sql, (start_time, end_time, 
                            top_15[0], top_15[1], top_15[2], top_15[3], top_15[4], 
                            top_15[5], top_15[6], top_15[7], top_15[8], top_15[9],
                            top_15[10], top_15[11], top_15[12], top_15[13], top_15[14]))

    def look_up_trade_by_exch(self, symbol, exch_name, start_time, end_time):
        table_name = "trade_of_{}".format(symbol)
        sql = "SELECT * from "
        sql += table_name
        sql += " where exch_name = %s and time > %s and time < %s"
        return self.fetch_sql_res(sql, (exch_name, start_time, end_time))

    def look_up_meta(self):
        """Look up meta data"""
        sql = "SELECT * from coin_meta"
        return self.fetch_sql_res(sql, [])
    
    def delete_trade_info(self, symbol):
        table_name = "trade_of_{}".format(symbol)
        sql = "DELETE from {}".format(table_name)
        self.exe_sql(sql)

    

def InitTable(db_name, symbols, exch_names):
    connector = CryptoCoinConnector(db_name)
    connector.create_meta_table()
    data = []
    for symbol in symbols:
        connector.create_trade_info_table(symbol)
    connector.close()
    # for exch_name in exch_names:
    #     for symbol in symbols:
    #         utctick = int(time.time())
    #         data.append( (exch_name, symbol, utctick) )
            
    # connector.insert_meta_data(data)