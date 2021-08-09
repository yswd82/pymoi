# -*- coding: UTF-8 -*-
import pyodbc
import pandas as pd
from core import PyMoi

# SQL Server 2019
driver = "{ODBC Driver 11 for SQL Server}"
server = "127.0.0.1\datahandling"
database = "db"
trusted_connection = "yes"
con_str = f"DRIVER={driver};SERVER={server};DATABASE={database};PORT=1433;Trusted_Connection={trusted_connection};"


table = 'dbo.pymoi_example'
header_col = ['fid', 'fdate', 'fcode', 'fprice', 'famount']

# insert initial data
csvfile = "table_init.csv"
df = pd.read_csv(csvfile, header=None, names=header_col)
df['record_id'] = range(len(df))
df['is_deleted'] = 0

print(len(df))

con = pyodbc.connect(con_str)
moi = PyMoi(con)
moi.to_sql(table, df)

# view table
cur = con.cursor()
cur.execute(f"SELECT * FROM {table}")
rows = cur.fetchall()
cur.close()

print('=====initial condition=====')
for row in rows:
    print(row)


# insert additional data
csvfile_ow1 = "overwrite_case1.csv"
df_ow1 = pd.read_csv(csvfile_ow1, header=None, names=header_col)

maxid = moi.get_max_record_id(table)

print('maxid=',maxid)
print('len1=', len(df_ow1))

df_ow1['record_id'] = range(maxid+1, maxid+1 + len(df_ow1)) 
df_ow1['is_deleted'] = 0

moi.to_sql(table, df_ow1)


# view table
cur = con.cursor()
cur.execute(f"SELECT * FROM {table}")
rows = cur.fetchall()
cur.close()

print('=====ow1 condition=====')
for row in rows:
    print(row)


# clear data
cur = con.cursor()
cur.execute(f"delete from {table}")
con.commit()
cur.close()
