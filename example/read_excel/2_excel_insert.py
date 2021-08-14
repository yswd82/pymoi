# -*- coding: UTF-8 -*-
import time
from pymoi.core import PyMoi
from pymoi.util import enginefactory_mssql_pyodbc
from pymoi.reader import ExcelReader, FixedParameter, CellParameter, DirectionParameter, RepeatParameter

driver = 'ODBC Driver 11 for SQL Server'
host = '127.0.0.1\datahandling'
database = 'db'

engine = enginefactory_mssql_pyodbc(
    server=host, port=1433, database=database, driver=driver)

filename = "example\\data\\init.xlsx"
header = ['fid', 'fdate', 'fprice', 'famount']

xlsrd = ExcelReader(fullname=filename, seek_start="B6",
                    sheetname="Sheet1", names=header)

params = [
    DirectionParameter(1, "B", 2),
    RepeatParameter(1, "D", 1),
    CellParameter("B3"),
]

xlsrd.parameters = params

df = xlsrd.read()
print(df.head(100))

start = time.time()

pm = PyMoi(bind=engine, name='pymoi_example')

pm.clear()

pm.execute(xlsrd)

elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time) + "[sec]")

df = pm.read_table()
print(df.head(100))
