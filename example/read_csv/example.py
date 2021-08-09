import time
import pyodbc
import pandas as pd
from pymoi import PyMoi

# SQL Server 2019
driver = "{ODBC Driver 11 for SQL Server}"
server = "127.0.0.1\datahandling"
database = "db"
trusted_connection = "yes"
con_str = f"DRIVER={driver};SERVER={server};DATABASE={database};PORT=1433;Trusted_Connection={trusted_connection};"

# make con object
con = pyodbc.connect(con_str)

csvfile = "example\\read_csv\\testdata_100000.csv"
df = pd.read_csv(csvfile)

start = time.time()

moi = PyMoi(con)
moi.to_sql("dbo.pymoi_example_read_csv", df)

elapsed_time = time.time() - start
print("elapsed_time:{0}".format(elapsed_time) + "[sec]")

# 100k lines 12sec
# 1000k lines 172-246s
