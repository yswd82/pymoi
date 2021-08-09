# -*- coding: UTF-8 -*-
import time
import pandas as pd
from sqlalchemy import create_engine
from pymoi import PyMoi2

dialect = 'mssql'
driver1 = 'pyodbc'
driver = 'ODBC Driver 11 for SQL Server'
host = '127.0.0.1\datahandling'
port = 1433
database = 'db'

engine = create_engine(
    f"{dialect}+{driver1}://{host}:{port}/{database}?trusted_connection=yes&driver={driver}")

csvfile = "example\\read_csv\\testdata_100000.csv"
df = pd.read_csv(csvfile)

start = time.time()

pm2 = PyMoi2(bind=engine, name='pymoi_example_read_csv')
pm2.execute(df)

elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time) + "[sec]")

pm2.show()

# 23sec
