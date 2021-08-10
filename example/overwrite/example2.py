# -*- coding: UTF-8 -*-
import pandas as pd
from sqlalchemy import create_engine
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.inspection import inspect
# from sqlalchemy.schema import Table
# from sqlalchemy.sql import func
# import pymoi
from pymoi import PyMoi2, OverwriteParameter

dialect = 'mssql'
driver1 = 'pyodbc'
driver = 'ODBC Driver 11 for SQL Server'
host = '127.0.0.1\datahandling'
port = 1433
database = 'db'

engine = create_engine(
    f"{dialect}+{driver1}://{host}:{port}/{database}?trusted_connection=yes&driver={driver}")


pm2 = PyMoi2(bind=engine, name='pymoi_example')

# テーブル初期化
pm2.clear()

# ヘッダー
header_col = ['fid', 'fdate', 'fcode', 'fprice', 'famount']

# 上書き条件
owkey = ['fid', 'fcode']
ow = OverwriteParameter(mode='physical', keys=owkey)


# 初期データ投入
csvfile = "example\\overwrite\\table_init.csv"
df = pd.read_csv(csvfile, header=None, names=header_col)
# pm2.execute(df, overwrite=owkey)
pm2.execute2(df, overwrite=ow)

# 結果
print("-----initial data inserted-----")
pm2.show(10)


# 追加データ投入
csvfile_ow1 = "example\\overwrite\\overwrite_case1.csv"
df_ow1 = pd.read_csv(csvfile_ow1, header=None, names=header_col)
# pm2.execute(df_ow1, overwrite=owkey)
pm2.execute2(df_ow1, overwrite=ow)

# 結果
print("-----last result-----")
pm2.show(10)
