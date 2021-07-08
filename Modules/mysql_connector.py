"""This module is to help to connect mysql DB"""
import pdb
import pandas as pd
import numpy as np
from tqdm import tqdm
import mysql.connector as mq


class BaseMySQLConnector():
    """Base abstract to connect to MySQL"""

    def __init__(self, db_name):
        """Init"""
        self.db = MySQLConnector.create_connection()
        self.create_database(db_name)

    def create_database(self, db_name):
        """Construct SQL to create DB"""
        sql = "CREATE DATABASE IF NOT EXISTS {}".format(db_name)
        self.exe_sql(sql)
        self.exe_sql("USE {}".format(db_name))

    @classmethod
    def create_connection(cls):
        """Create connection to DB"""
        try:
            return mq.connect(host="localhost", user="root", passwd="123456")
        except RuntimeError as e:
            print(e)

    def fetch_sql_res(self, sql, vals):
        """Execute SQL and return the results"""
        try:
            cursor = self.db.cursor()
            cursor.execute(sql, vals)
            res = cursor.fetchall()
            cursor.close()
            return res
        except RuntimeError as e:
            print(e)

    def exe_sql(self, sql):
        """Execute the SQL"""
        try:
            cursor = self.db.cursor()
            cursor.execute(sql)
            cursor.close()
            self.db.commit()
        except RuntimeError as e:
            print(e)

    def exe_sql_many(self, sql, vals):
        """Execute SQL with many values"""
        try:
            cursor = self.db.cursor()
            cursor.executemany(sql, vals)
            cursor.close()
            self.db.commit()
        except RuntimeError as e:
            print(e)

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

    def create_trade_info_table(self, symbol):
        """Create table of trade by tick in binance"""
        table_name = "trade_of_{}".format(symbol)
        sql = "CREATE TABLE IF NOT EXISTS {}".format(table_name)
        sql += " (exch_name VARCHAR(255), id VARCHAR(255), price DOUBLE, qty DOUBLE, quoteQty DOUBLE, time BIGINT(8) unsigned, isBuyerMaker BOOL, PRIMARY KEY (exch_name, id))"
        self.exe_sql(sql)
        print("Tables Created Successfully!")

    def insert_trade_data(self, data, symbol):
        table_name = "trade_of_{}".format(symbol)
        sql = "INSERT IGNORE INTO {} (exch_name, id, price, qty, quoteQty, time, isBuyerMaker) ".format(table_name)
        sql += "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        self.exe_sql_many(sql, data)

    def look_up_trade_info(self, symbol, start_time, end_time):
        table_name = "trade_of_{}".format(symbol)
        sql = "SELECT * from "
        sql += table_name
        sql += " where time > %s and time < %s"
        return self.fetch_sql_res(sql, (start_time, end_time))
    
    def look_up_meta(self):
        """Look up meta data"""
        sql = "SELECT * from coin_meta"
        return self.fetch_sql_res(sql, [])
    
