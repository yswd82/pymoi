# -*- coding: UTF-8 -*-
import time
from pymoi.core import PyMoi
from pymoi.util import enginefactory_mssql_pyodbc
from pymoi.reader import CsvReader

driver = 'ODBC Driver 11 for SQL Server'
host = '127.0.0.1\datahandling'
database = 'db'

engine = enginefactory_mssql_pyodbc(
    server=host, port=1433, database=database, driver=driver)

csvfile = "example\\data\\init.csv"
csvrd = CsvReader(csvfile)

# CSVをDataFrameに変換した状態を確認
df = csvrd.read()
print(df.head(100))

start = time.time()

pm = PyMoi(bind=engine, name='pymoi_example')

pm.clear()

df = pm.read_table()
print(df.head(100))

pm.execute(csvrd)

elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time) + "[sec]")

# insert後のテーブル内容を確認
df = pm.read_table()
print(df.head(100))
