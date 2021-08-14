# -*- coding: UTF-8 -*-
import time
import pandas as pd
from sqlalchemy import create_engine
from pymoi.core import PyMoi, OverwriteParameter
from pymoi.util import enginefactory_mssql_pyodbc
from pymoi.reader import CsvReader


driver = 'ODBC Driver 11 for SQL Server'
host = '127.0.0.1\datahandling'
database = 'db'

engine = enginefactory_mssql_pyodbc(
    server=host, port=1433, database=database, driver=driver)

start = time.time()

pm = PyMoi(bind=engine, name='pymoi_example_ow')

# テーブル初期化
pm.clear()

# 初期データ読込
csvfile = "example\\data\\init.csv"
csvrd = CsvReader(csvfile)

# 初期データ内容を確認
df = csvrd.read()
print(df.head(100))


# 上書き条件
owkey = ['fid', 'fcode']
ow = OverwriteParameter(mode='physical', keys=owkey)

# 初期データ投入
pm.execute(csvrd, overwrite=ow)

elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time) + "[sec]")

# insert後のテーブル内容を確認
df = pm.read_table()
print(df.head(100))


# 追加データ読込
csvfile_ow = "example\\data\\overwrite_case1.csv"
csvrd_ow = CsvReader(csvfile_ow)

# 追加データ内容確認
df = csvrd_ow.read()
print(df.head(100))


# 追加データ投入
pm.execute(csvrd_ow, overwrite=ow)

# insert後のテーブル内容を確認
df = pm.read_table()
print(df.head(100))
