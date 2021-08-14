# -*- coding: UTF-8 -*-
import time
from pymoi import PyMoi
from pymoi.util import enginefactory_mssql_pyodbc
from pymoi.reader import CsvReader

driver = 'ODBC Driver 11 for SQL Server'
host = '127.0.0.1\datahandling'
database = 'db'

# SQL Alchemy
engine = enginefactory_mssql_pyodbc(
    server=host, port=1433, database=database, driver=driver)

csvfile = "example\\read_csv\\testdata_100.csv"
csvrd = CsvReader(csvfile)


start = time.time()

pm = PyMoi(bind=engine, name='pymoi_example_read_csv')

pm.clear()

pm.execute(csvrd)

elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time) + "[sec]")

df = pm.read_table()
print(df.head(100))

# 100 lines 0.5sec
# 100k lines 13sec
# 1000k lines 246sec-313sec
# 1000k lines 158sec-182sec (core)
